from abc import ABC
from datetime import timedelta
from typing import Mapping, Optional, Sequence

from snuba import state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.pipeline.single_query_plan_pipeline import SingleQueryPlanPipelineBuilder
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import (
    QueryStorageSelector,
    StorageAndMappers,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.request.request_settings import RequestSettings


event_translator = TranslationMappers(
    columns=[
        ColumnToMapping(None, "release", None, "tags", "sentry:release"),
        ColumnToMapping(None, "dist", None, "tags", "sentry:dist"),
        ColumnToMapping(None, "user", None, "tags", "sentry:user"),
    ],
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
    ],
)


class EventsQueryStorageSelector(QueryStorageSelector):
    def __init__(self, mappers: TranslationMappers) -> None:
        self.__events_table = get_writable_storage(StorageKey.EVENTS)
        self.__events_ro_table = get_storage(StorageKey.EVENTS_RO)
        self.__mappers = mappers

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not request_settings.get_consistent()
        )

        storage = (
            self.__events_ro_table if use_readonly_storage else self.__events_table
        )
        return StorageAndMappers(storage, self.__mappers)


class BaseEventsEntity(Entity, ABC):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        storage = get_writable_storage(StorageKey.EVENTS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        super().__init__(
            storages=[storage],
            query_pipeline_builder=SingleQueryPlanPipelineBuilder(
                query_plan_builder=SelectedStorageQueryPlanBuilder(
                    selector=EventsQueryStorageSelector(
                        mappers=event_translator
                        if custom_mappers is None
                        else event_translator.concat(custom_mappers)
                    )
                ),
            ),
            abstract_column_set=columns,
            join_relationships={},
            writable_storage=storage,
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            TimeSeriesProcessor(
                {"time": "timestamp", "rtime": "received"}, ("timestamp", "received")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            HandledFunctionsProcessor(
                "exception_stacks.mechanism_handled", self.get_data_model()
            ),
        ]


class EventsEntity(BaseEventsEntity):
    pass
