from abc import ABC
from typing import List, Optional, Sequence, Tuple

from snuba import settings, state
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.clickhouse_upgrade import Option, RolloutSelector
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.single_storage import SelectedStorageQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query import ProcessableQuery
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.handled_functions import HandledFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.tags_expander import TagsExpanderProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.query_settings import QuerySettings
from snuba.query.validation.validators import EntityRequiredColumnValidator

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
                    None,
                    "IPv4NumToString",
                    (Column(None, None, "ip_address_v4"),),
                ),
                FunctionCall(
                    None,
                    "IPv6NumToString",
                    (Column(None, None, "ip_address_v6"),),
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


class ErrorsQueryStorageSelector(QueryStorageSelector):
    def __init__(self, mappers: TranslationMappers) -> None:
        self.__errors_table = get_writable_storage(StorageKey.ERRORS)
        self.__errors_ro_table = get_storage(StorageKey.ERRORS_RO)
        self.__mappers = mappers

    def select_storage(
        self, query: Query, query_settings: QuerySettings
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        storage = (
            self.__errors_ro_table if use_readonly_storage else self.__errors_table
        )
        return StorageAndMappers(storage, self.__mappers)


class ErrorsV2QueryStorageSelector(QueryStorageSelector):
    def __init__(self, mappers: TranslationMappers) -> None:
        self.__errors_table = get_writable_storage(StorageKey.ERRORS_V2)
        self.__errors_ro_table = get_storage(StorageKey.ERRORS_V2_RO)
        self.__mappers = mappers

    def select_storage(
        self, query: Query, query_settings: QuerySettings
    ) -> StorageAndMappers:
        use_readonly_storage = not query_settings.get_consistent()

        storage = (
            self.__errors_ro_table if use_readonly_storage else self.__errors_table
        )
        return StorageAndMappers(storage, self.__mappers)


def v2_selector_function(query: Query, referrer: str) -> Tuple[str, List[str]]:
    if settings.ERRORS_UPGRADE_BEGINING_OF_TIME is None or not isinstance(
        query, ProcessableQuery
    ):
        return ("errors_v1", [])

    range = get_time_range(query, "timestamp")
    if range[0] is None or range[0] < settings.ERRORS_UPGRADE_BEGINING_OF_TIME:
        return ("errors_v1", [])

    mapping = {
        Option.ERRORS: "errors_v1",
        Option.ERRORS_V2: "errors_v2",
    }
    choice = RolloutSelector(Option.ERRORS, Option.ERRORS_V2, "errors").choose(referrer)
    if choice.secondary is None:
        return (mapping[choice.primary], [])
    else:
        return (mapping[choice.primary], [mapping[choice.secondary]])


class BaseEventsEntity(Entity, ABC):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        v1_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=ErrorsQueryStorageSelector(
                    mappers=errors_translators
                    if custom_mappers is None
                    else errors_translators.concat(custom_mappers)
                )
            )
        )
        v2_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=ErrorsV2QueryStorageSelector(
                    mappers=errors_translators
                    if custom_mappers is None
                    else errors_translators.concat(custom_mappers)
                )
            )
        )

        events_storage = get_writable_storage(StorageKey.ERRORS)
        pipeline_builder: QueryPipelineBuilder[ClickhouseQueryPlan] = PipelineDelegator(
            query_pipeline_builders={
                "errors_v1": v1_pipeline_builder,
                "errors_v2": v2_pipeline_builder,
            },
            selector_func=v2_selector_function,
            split_rate_limiter=True,
            ignore_secondary_exceptions=True,
        )

        # TODO: entity data model is using ClickHouse columns/schema, should be corrected
        schema = events_storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        super().__init__(
            storages=[events_storage],
            query_pipeline_builder=pipeline_builder,
            abstract_column_set=columns,
            join_relationships={
                "grouped": JoinRelationship(
                    rhs_entity=EntityKey.GROUPEDMESSAGE,
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
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            TimeSeriesProcessor(
                {"time": "timestamp", "rtime": "received"}, ("timestamp", "received")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            HandledFunctionsProcessor("exception_stacks.mechanism_handled"),
            ReferrerRateLimiterProcessor(),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]


class EventsEntity(BaseEventsEntity):
    pass
