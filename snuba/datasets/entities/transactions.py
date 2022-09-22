from abc import ABC
from typing import List, Optional, Sequence, Tuple

from snuba import settings, state
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.clickhouse_upgrade import Option, RolloutSelector
from snuba.datasets.entity import Entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.datasets.plans.single_storage import (
    SelectedStorageQueryPlanBuilder,
    SingleStorageQueryPlanBuilder,
)
from snuba.datasets.storage import QueryStorageSelector, StorageAndMappers
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.query_pipeline import QueryPipelineBuilder
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query import ProcessableQuery
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.tags_expander import TagsExpanderProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.processors.performance_expressions import (
    apdex_processor,
    failure_rate_processor,
)
from snuba.query.query_settings import QuerySettings
from snuba.query.validation.validators import EntityRequiredColumnValidator

transaction_translator = TranslationMappers(
    columns=[
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
        ColumnToFunction(
            None, "user", "nullIf", (Column(None, None, "user"), Literal(None, ""))
        ),
        # These column aliases originally existed in the ``discover`` dataset,
        # but now live here to maintain compatibility between the composite
        # ``discover`` dataset and the standalone ``transaction`` dataset. In
        # the future, these aliases should be defined on the Transaction entity
        # instead of the dataset.
        ColumnToLiteral(None, "type", "transaction"),
        ColumnToColumn(None, "timestamp", None, "finish_ts"),
        ColumnToColumn(None, "username", None, "user_name"),
        ColumnToColumn(None, "email", None, "user_email"),
        ColumnToColumn(None, "transaction", None, "transaction_name"),
        ColumnToColumn(None, "message", None, "transaction_name"),
        ColumnToColumn(None, "title", None, "transaction_name"),
        ColumnToColumn(None, "spans.exclusive_time", None, "spans.exclusive_time_32"),
        ColumnToColumn(None, "app_start_type", None, "app_start_type"),
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
        SubscriptableMapper(None, "measurements", None, "measurements", nullable=True),
        SubscriptableMapper(
            None, "span_op_breakdowns", None, "span_op_breakdowns", nullable=True
        ),
    ],
)


class TransactionsQueryStorageSelector(QueryStorageSelector):
    def __init__(self, mappers: TranslationMappers) -> None:
        self.__transactions_table = get_writable_storage(StorageKey.TRANSACTIONS)
        self.__transactions_ro_table = get_storage(StorageKey.TRANSACTIONS_RO)
        self.__mappers = mappers

    def select_storage(
        self, query: Query, query_settings: QuerySettings
    ) -> StorageAndMappers:
        readonly_referrer = (
            query_settings.referrer
            in settings.TRANSACTIONS_DIRECT_TO_READONLY_REFERRERS
        )
        use_readonly_storage = readonly_referrer or state.get_config(
            "enable_transactions_readonly_table", False
        )
        storage = (
            self.__transactions_ro_table
            if use_readonly_storage
            else self.__transactions_table
        )
        return StorageAndMappers(storage, self.__mappers)


def v2_selector_function(query: Query, referrer: str) -> Tuple[str, List[str]]:
    if settings.TRANSACTIONS_UPGRADE_BEGINING_OF_TIME is None or not isinstance(
        query, ProcessableQuery
    ):
        return ("transactions_v1", [])

    time_range = get_time_range(query, "timestamp")
    if time_range == (None, None):
        time_range = get_time_range(query, "finish_ts")
    if (
        time_range[0] is None
        or time_range[0] < settings.TRANSACTIONS_UPGRADE_BEGINING_OF_TIME
    ):
        return ("transactions_v1", [])

    mapping = {
        Option.TRANSACTIONS: "transactions_v1",
        Option.TRANSACTIONS_V2: "transactions_v2",
    }
    choice = RolloutSelector(
        Option.TRANSACTIONS, Option.TRANSACTIONS_V2, "transactions"
    ).choose(referrer)
    if choice.secondary is None:
        return (mapping[choice.primary], [])
    else:
        return (mapping[choice.primary], [mapping[choice.secondary]])


class BaseTransactionsEntity(Entity, ABC):
    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        storage = get_writable_storage(StorageKey.TRANSACTIONS)
        schema = storage.get_table_writer().get_schema()
        mappers = (
            transaction_translator
            if custom_mappers is None
            else transaction_translator.concat(custom_mappers)
        )

        v1_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=TransactionsQueryStorageSelector(mappers=mappers)
            ),
        )

        v2_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=get_storage(StorageKey.TRANSACTIONS_V2),
                mappers=mappers,
            )
        )

        pipeline_builder: QueryPipelineBuilder[ClickhouseQueryPlan] = PipelineDelegator(
            query_pipeline_builders={
                "transactions_v1": v1_pipeline_builder,
                "transactions_v2": v2_pipeline_builder,
            },
            selector_func=v2_selector_function,
            split_rate_limiter=True,
            ignore_secondary_exceptions=True,
        )

        super().__init__(
            storages=[storage],
            query_pipeline_builder=pipeline_builder,
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="finish_ts",
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            TimeSeriesProcessor(
                {"time": "finish_ts"}, ("start_ts", "finish_ts", "timestamp")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            apdex_processor(),
            failure_rate_processor(),
            ReferrerRateLimiterProcessor(),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]


class TransactionsEntity(BaseTransactionsEntity):
    pass
