import random
from abc import ABC
from datetime import timedelta
from functools import partial
from typing import List, Mapping, Optional, Sequence, Tuple

import sentry_sdk
from snuba import environment, settings, state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.entities.assign_reason import assign_reason_category
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import (
    QueryStorageSelector,
    StorageAndMappers,
    WritableTableStorage,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.expressions import Column, FunctionCall
from snuba.query.extensions import QueryExtension
from snuba.query.formatters.tracing import format_query
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

metrics = MetricsWrapper(environment.metrics, "snuplicator")


def callback_func(
    storage: str,
    query: Query,
    request_settings: RequestSettings,
    referrer: str,
    results: List[Result[QueryResult]],
) -> None:
    cache_hit = False
    is_duplicate = False

    # Captures if any of the queries involved was a cache hit or duplicate, as cache
    # hits may a cause of inconsistency between results.
    # Doesn't attempt to distinguish between all of the specific scenarios (one or both
    # queries, or splits of those queries could have hit the cache).
    if any([result.result.extra["stats"].get("cache_hit", 0) for result in results]):
        cache_hit = True
    elif any(
        [result.result.extra["stats"].get("is_duplicate", 0) for result in results]
    ):
        is_duplicate = True

    consistent = request_settings.get_consistent()

    if not results:
        metrics.increment(
            "query_result",
            tags={"storage": storage, "match": "empty", "referrer": referrer},
        )
        return

    primary_result = results.pop(0)
    primary_result_data = primary_result.result.result["data"]

    for result in results:
        result_data = result.result.result["data"]

        metrics.timing(
            "diff_ms",
            round((result.execution_time - primary_result.execution_time) * 1000),
            tags={
                "referrer": referrer,
                "cache_hit": str(cache_hit),
                "is_duplicate": str(is_duplicate),
                "consistent": str(consistent),
            },
        )

        # Do not bother diffing the actual results of sampled queries
        if request_settings.get_turbo() or query.get_sample() not in [None, 1.0]:
            return

        if result_data == primary_result_data:
            metrics.increment(
                "query_result",
                tags={
                    "storage": storage,
                    "match": "true",
                    "referrer": referrer,
                    "cache_hit": str(cache_hit),
                    "is_duplicate": str(is_duplicate),
                    "consistent": str(consistent),
                },
            )
        else:
            # Do not log cache hits to Sentry as it creates too much noise
            if cache_hit:
                continue

            reason = assign_reason_category(result_data, primary_result_data, referrer)

            metrics.increment(
                "query_result",
                tags={
                    "storage": storage,
                    "match": "false",
                    "referrer": referrer,
                    "reason": reason,
                    "cache_hit": str(cache_hit),
                    "is_duplicate": str(is_duplicate),
                    "consistent": str(consistent),
                },
            )

            if len(result_data) != len(primary_result_data):
                sentry_sdk.capture_message(
                    f"Non matching {storage} result - different length",
                    level="warning",
                    tags={
                        "referrer": referrer,
                        "storage": storage,
                        "reason": reason,
                        "cache_hit": str(cache_hit),
                        "is_duplicate": str(is_duplicate),
                        "consistent": str(consistent),
                    },
                    extras={
                        "query": format_query(query),
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
                        tags={
                            "referrer": referrer,
                            "storage": storage,
                            "reason": reason,
                            "cache_hit": str(cache_hit),
                            "is_duplicate": str(is_duplicate),
                            "consistent": str(consistent),
                        },
                        extras={
                            "query": format_query(query),
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
        events_storage = get_writable_storage(StorageKey.EVENTS)
        errors_storage = get_writable_storage(StorageKey.ERRORS)
        schema = events_storage.get_table_writer().get_schema()
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

        def selector_func(_query: Query, referrer: str) -> Tuple[str, List[str]]:
            # In case something goes wrong, set this to 1 to revert to the events storage.
            kill_rollout = state.get_config("errors_rollout_killswitch", 0)
            assert isinstance(kill_rollout, (int, str))
            if int(kill_rollout):
                return "events", []

            if referrer in settings.ERRORS_ROLLOUT_BY_REFERRER:
                return "errors", []

            if settings.ERRORS_ROLLOUT_ALL:
                return "errors", []

            default_threshold = state.get_config("errors_query_percentage", 0)
            assert isinstance(default_threshold, (float, int, str))
            threshold = settings.ERRORS_QUERY_PERCENTAGE_BY_REFERRER.get(
                referrer, default_threshold
            )

            if random.random() < float(threshold):
                return "events", ["errors"]

            return "events", []

        def writable_storage() -> WritableTableStorage:
            if settings.ERRORS_ROLLOUT_WRITABLE_STORAGE:
                return get_writable_storage(StorageKey.ERRORS)
            else:
                return get_writable_storage(StorageKey.EVENTS)

        super().__init__(
            storages=[events_storage, errors_storage],
            query_pipeline_builder=PipelineDelegator(
                query_pipeline_builders={
                    "events": events_pipeline_builder,
                    "errors": errors_pipeline_builder,
                },
                selector_func=selector_func,
                callback_func=partial(callback_func, "errors"),
            ),
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
            writable_storage=writable_storage(),
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
