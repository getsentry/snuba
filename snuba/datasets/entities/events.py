from abc import ABC
from datetime import timedelta
from typing import Mapping, Optional, Sequence

from snuba import settings, state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.expressions import Column, FunctionCall
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
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

errors_translators = TranslationMappers(
    columns=[
        ColumnToMapping(None, "release", None, "tags", "sentry:release"),
        ColumnToMapping(None, "dist", None, "tags", "sentry:dist"),
        ColumnToMapping(None, "user", None, "tags", "sentry:user"),
        ColumnToFunction(
            None,
            "ip_address",
            "coalesce",
            (
                FunctionCall(
                    None, "IPv4NumToString", (Column(None, None, "ip_address_v4"),),
                ),
                FunctionCall(
                    None, "IPv6NumToString", (Column(None, None, "ip_address_v6"),),
                ),
            ),
        ),
        ColumnToColumn(None, "transaction", None, "transaction_name"),
        ColumnToColumn(None, "username", None, "user_name"),
        ColumnToColumn(None, "email", None, "user_email"),
        ColumnToMapping(
            None,
            "geo_country_code",
            None,
            "contexts",
            "geo.country_code",
            nullable=True,
        ),
        ColumnToMapping(
            None, "geo_region", None, "contexts", "geo.region", nullable=True
        ),
        ColumnToMapping(None, "geo_city", None, "contexts", "geo.city", nullable=True),
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


class ErrorsQueryStorageSelector(QueryStorageSelector):
    def __init__(self, mappers: TranslationMappers) -> None:
        self.__errors_table = get_writable_storage(StorageKey.ERRORS)
        self.__errors_ro_table = get_storage(StorageKey.ERRORS_RO)
        self.__mappers = mappers

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not request_settings.get_consistent()
        )

        storage = (
            self.__errors_ro_table if use_readonly_storage else self.__errors_table
        )
        return StorageAndMappers(storage, self.__mappers)


class BaseEventsEntity(Entity, ABC):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        if settings.ERRORS_ROLLOUT_ALL:
            events_storage = get_writable_storage(StorageKey.ERRORS)
            pipeline_builder = SimplePipelineBuilder(
                query_plan_builder=SelectedStorageQueryPlanBuilder(
                    selector=ErrorsQueryStorageSelector(
                        mappers=errors_translators
                        if custom_mappers is None
                        else errors_translators.concat(custom_mappers)
                    )
                ),
            )
        else:
            events_storage = get_writable_storage(StorageKey.EVENTS)
            pipeline_builder = SimplePipelineBuilder(
                query_plan_builder=SelectedStorageQueryPlanBuilder(
                    selector=EventsQueryStorageSelector(
                        mappers=event_translator
                        if custom_mappers is None
                        else event_translator.concat(custom_mappers)
                    )
                ),
            )

        schema = events_storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        super().__init__(
            storages=[events_storage],
            query_pipeline_builder=pipeline_builder,
            abstract_column_set=columns,
            join_relationships={
                "grouped": JoinRelationship(
                    rhs_entity=EntityKey.GROUPEDMESSAGES,
                    columns=[("project_id", "project_id"), ("group_id", "id")],
                    join_type=JoinType.INNER,
                    equivalences=[],
                ),
                "assigned": JoinRelationship(
                    rhs_entity=EntityKey.GROUPASSIGNEE,
                    columns=[("project_id", "project_id"), ("group_id", "group_id")],
                    join_type=JoinType.INNER,
                    equivalences=[],
                ),
            },
            writable_storage=events_storage,
            required_filter_columns=["project_id"],
            required_time_column="timestamp",
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
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]


class EventsEntity(BaseEventsEntity):
    pass
