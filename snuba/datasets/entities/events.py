import random
from abc import ABC
from datetime import timedelta
from typing import List, Mapping, Optional, Sequence, Tuple

import sentry_sdk

from functools import partial
from snuba import environment, state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.expressions import Column, Literal
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
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult

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
        ColumnToFunction(
            None, "user", "nullIf", (Column(None, None, "user"), Literal(None, ""))
        ),
        ColumnToColumn(None, "username", None, "user_name"),
        ColumnToColumn(None, "email", None, "user_email"),
        ColumnToMapping(None, "geo_country_code", None, "contexts", "geo.country_code"),
        ColumnToMapping(None, "geo_region", None, "contexts", "geo.region"),
        ColumnToMapping(None, "geo_city", None, "contexts", "geo.city"),
    ],
    subscriptables=[
        SubscriptableMapper(None, "tags", None, "tags"),
        SubscriptableMapper(None, "contexts", None, "contexts"),
    ],
)

metrics = MetricsWrapper(environment.metrics, "snuplicator")


def callback_func(
    storage: str, query: Query, referrer: str, results: List[Result[QueryResult]]
) -> None:
    primary_result = results.pop(0)
    primary_result_data = primary_result.result.result["data"]

    for result in results:
        result_data = result.result.result["data"]

        if result_data == primary_result_data:
            metrics.increment(
                "query_result", tags={"storage": storage, "match": "true"}
            )
        else:
            metrics.increment(
                "query_result", tags={"storage": storage, "match": "false"}
            )

            if len(result_data) != len(primary_result_data):
                sentry_sdk.capture_message(
                    f"Non matching {storage} result - different length",
                    level="warning",
                    tags={"referrer": referrer, "storage": storage},
                    extras={
                        "query": query.get_body(),
                        "primary_result": len(primary_result_data),
                        "other_result": len(result_data),
                    },
                )

                break

            # Avoid sending too much data to Sentry - just one row for now
            for idx in range(len(result_data)):
                if result_data[idx] != primary_result_data[idx]:
                    sentry_sdk.capture_message(
                        "Non matching result - different result",
                        level="warning",
                        tags={"referrer": referrer, "storage": storage},
                        extras={
                            "query": query.get_body(),
                            "primary_result": primary_result_data[idx],
                            "other_result": result_data[idx],
                        },
                    )

                    break


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
        storage = get_writable_storage(StorageKey.EVENTS)
        schema = storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        events_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=EventsQueryStorageSelector(
                    mappers=event_translator
                    if custom_mappers is None
                    else event_translator.concat(custom_mappers)
                )
            ),
        )

        errors_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=ErrorsQueryStorageSelector(
                    mappers=errors_translators
                    if custom_mappers is None
                    else errors_translators.concat(custom_mappers)
                )
            ),
        )

        def selector_func(_query: Query) -> Tuple[str, List[str]]:
            if random.random() < float(state.get_config("errors_query_percentage", 0)):
                return "events", ["errors"]

            return "events", []

        super().__init__(
            storages=[storage],
            query_pipeline_builder=PipelineDelegator(
                query_pipeline_builders={
                    "events": events_pipeline_builder,
                    "errors": errors_pipeline_builder,
                },
                selector_func=selector_func,
                callback_func=partial(callback_func, "errors"),
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
