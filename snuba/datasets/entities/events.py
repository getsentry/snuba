from abc import ABC
from typing import Optional, Sequence

from snuba import state
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.validators import AggregationValidator
from snuba.datasets.plans.storage_builder import StorageQueryPlanBuilder
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
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
    def __init__(self, errors_translators: TranslationMappers) -> None:
        self.__errors_table = get_writable_storage(StorageKey.ERRORS)
        self.__errors_ro_table = get_storage(StorageKey.ERRORS_RO)
        self.__mappers = errors_translators

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
    ) -> StorageAndMappers:
        use_readonly_storage = (
            state.get_config("enable_events_readonly_table", False)
            and not query_settings.get_consistent()
        )

        if use_readonly_storage:
            return StorageAndMappers(self.__errors_ro_table, self.__mappers, False)
        return StorageAndMappers(self.__errors_table, self.__mappers, True)


class BaseEventsEntity(Entity, ABC):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        events_storage = get_writable_storage(StorageKey.ERRORS)
        events_read_storage = get_storage(StorageKey.ERRORS_RO)
        mappers = (
            errors_translators
            if custom_mappers is None
            else errors_translators.concat(custom_mappers)
        )

        pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=StorageQueryPlanBuilder(
                storages=[
                    StorageAndMappers(events_read_storage, mappers, False),
                    StorageAndMappers(events_storage, mappers, True),
                ],
                selector=ErrorsQueryStorageSelector(mappers),
            ),
        )
        schema = events_storage.get_table_writer().get_schema()
        columns = schema.get_columns()

        super().__init__(
            storages=[events_storage, events_read_storage],
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
            subscription_processors=None,
            subscription_validators=[
                AggregationValidator(1, ["groupby", "having", "orderby"], "timestamp")
            ],
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
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
