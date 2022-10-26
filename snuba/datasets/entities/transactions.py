from abc import ABC
from typing import Optional, Sequence

from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import BaseEntitySubscription, Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.logical.custom_function import (
    ApdexProcessor,
    FailureRateProcessor,
)
from snuba.query.processors.logical.object_id_rate_limiter import (
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.tags_expander import TagsExpanderProcessor
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
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


class BaseTransactionsEntity(Entity, ABC):
    def __init__(self, custom_mappers: Optional[TranslationMappers] = None) -> None:
        storage = get_writable_storage(StorageKey.TRANSACTIONS)
        schema = storage.get_table_writer().get_schema()

        pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=get_storage(StorageKey.TRANSACTIONS),
                mappers=transaction_translator
                if custom_mappers is None
                else transaction_translator.concat(custom_mappers),
            )
        )

        super().__init__(
            storages=[storage],
            query_pipeline_builder=pipeline_builder,
            abstract_column_set=schema.get_columns(),
            join_relationships={},
            writable_storage=storage,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="finish_ts",
            entity_subscription=BaseEntitySubscription(
                1, ["groupby", "having", "orderby"]
            ),
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            TimeSeriesProcessor(
                {"time": "finish_ts"}, ("start_ts", "finish_ts", "timestamp")
            ),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            ApdexProcessor(),
            FailureRateProcessor(),
            ReferrerRateLimiterProcessor(),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]


class TransactionsEntity(BaseTransactionsEntity):
    pass
