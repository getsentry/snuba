from abc import ABC
from typing import Optional, Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    ColumnSet,
    DateTime,
    Float,
    Nested,
    SchemaModifiers,
    UInt,
)
from snuba.clickhouse.translators.snuba.function_call_mappers import (
    AggregateCurriedFunctionMapper,
    AggregateFunctionMapper,
)
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.processors import (
    AddColumnCondition,
    EntitySubscriptionProcessor,
)
from snuba.datasets.entity_subscriptions.validators import (
    AggregationValidator,
    EntitySubscriptionValidator,
)
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.granularity_processor import GranularityProcessor
from snuba.query.processors.logical.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
    ProjectReferrerRateLimiter,
    ReferrerRateLimiterProcessor,
)
from snuba.query.processors.logical.quota_processor import ResourceQuotaProcessor
from snuba.query.processors.logical.tags_type_transformer import TagsTypeTransformer
from snuba.query.processors.logical.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    EntityRequiredColumnValidator,
    GranularityValidator,
    QueryValidator,
)


class MetricsEntity(Entity, ABC):
    def __init__(
        self,
        writable_storage_key: Optional[StorageKey],
        readable_storage_key: StorageKey,
        value_schema: Sequence[Column[SchemaModifiers]],
        mappers: TranslationMappers,
        abstract_column_set: Optional[ColumnSet] = None,
        validators: Optional[Sequence[QueryValidator]] = None,
        subscription_processors: Optional[Sequence[EntitySubscriptionProcessor]] = None,
        subscription_validators: Optional[Sequence[EntitySubscriptionValidator]] = None,
    ) -> None:
        writable_storage = (
            get_writable_storage(writable_storage_key) if writable_storage_key else None
        )
        readable_storage = get_storage(readable_storage_key)
        storages = [readable_storage]
        if writable_storage:
            storages.append(writable_storage)

        if abstract_column_set is None:
            abstract_column_set = ColumnSet(
                [
                    Column("org_id", UInt(64)),
                    Column("project_id", UInt(64)),
                    Column("metric_id", UInt(64)),
                    Column("timestamp", DateTime()),
                    Column("bucketed_time", DateTime()),
                    Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
                    *value_schema,
                ]
            )

        if validators is None:
            validators = [
                EntityRequiredColumnValidator({"org_id", "project_id"}),
                GranularityValidator(minimum=10),
            ]

        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=SingleStorageQueryPlanBuilder(
                    readable_storage,
                    mappers=TranslationMappers(
                        subscriptables=[
                            SubscriptableMapper(None, "tags", None, "tags"),
                        ],
                    ).concat(mappers),
                )
            ),
            abstract_column_set=abstract_column_set,
            join_relationships={},
            writable_storage=writable_storage,
            validators=validators,
            required_time_column="timestamp",
            subscription_processors=subscription_processors,
            subscription_validators=subscription_validators,
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            GranularityProcessor(),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
            TagsTypeTransformer(),
        ]


class MetricsSetsEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_RAW,
            readable_storage_key=StorageKey.METRICS_SETS,
            value_schema=[
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ],
            mappers=TranslationMappers(
                functions=[
                    FunctionNameMapper("uniq", "uniqCombined64Merge"),
                    FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
                ],
            ),
            subscription_processors=[AddColumnCondition("organization", "org_id")],
            subscription_validators=[
                AggregationValidator(3, ["having", "orderby"], "timestamp")
            ],
        )


class MetricsCountersEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_RAW,
            readable_storage_key=StorageKey.METRICS_COUNTERS,
            value_schema=[Column("value", AggregateFunction("sum", [Float(64)]))],
            mappers=TranslationMappers(
                functions=[
                    FunctionNameMapper("sum", "sumMerge"),
                    FunctionNameMapper("sumIf", "sumMergeIf"),
                ],
            ),
            subscription_processors=[AddColumnCondition("organization", "org_id")],
            subscription_validators=[
                AggregationValidator(3, ["having", "orderby"], "timestamp")
            ],
        )


class OrgMetricsCountersEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=None,
            readable_storage_key=StorageKey.ORG_METRICS_COUNTERS,
            value_schema=[],
            mappers=TranslationMappers(),
            abstract_column_set=ColumnSet(
                [
                    Column("org_id", UInt(64)),
                    Column("project_id", UInt(64)),
                    Column("metric_id", UInt(64)),
                    Column("timestamp", DateTime()),
                    Column("bucketed_time", DateTime()),
                ]
            ),
            validators=[GranularityValidator(minimum=3600)],
            subscription_processors=None,
            subscription_validators=None,
        )


class MetricsDistributionsEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_RAW,
            readable_storage_key=StorageKey.METRICS_DISTRIBUTIONS,
            value_schema=[
                Column(
                    "percentiles",
                    AggregateFunction(
                        "quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]
                    ),
                ),
                Column("min", AggregateFunction("min", [Float(64)])),
                Column("max", AggregateFunction("max", [Float(64)])),
                Column("avg", AggregateFunction("avg", [Float(64)])),
                Column("sum", AggregateFunction("sum", [Float(64)])),
                Column("count", AggregateFunction("count", [Float(64)])),
                Column(
                    "histogram_buckets",
                    AggregateFunction("histogram(250)", [Float(64)]),
                ),
            ],
            mappers=TranslationMappers(
                functions=[
                    AggregateFunctionMapper("value", "min", "minMerge", "min"),
                    AggregateFunctionMapper("value", "minIf", "minMergeIf", "min"),
                    AggregateFunctionMapper("value", "max", "maxMerge", "max"),
                    AggregateFunctionMapper("value", "maxIf", "maxMergeIf", "max"),
                    AggregateFunctionMapper("value", "avg", "avgMerge", "avg"),
                    AggregateFunctionMapper("value", "avgIf", "avgMergeIf", "avg"),
                    AggregateFunctionMapper("value", "sum", "sumMerge", "sum"),
                    AggregateFunctionMapper("value", "sumIf", "sumMergeIf", "sum"),
                    AggregateFunctionMapper("value", "count", "countMerge", "count"),
                    AggregateFunctionMapper(
                        "value", "countIf", "countMergeIf", "count"
                    ),
                ],
                curried_functions=[
                    AggregateCurriedFunctionMapper(
                        "value", "quantiles", "quantilesMerge", "percentiles"
                    ),
                    AggregateCurriedFunctionMapper(
                        "value", "quantilesIf", "quantilesMergeIf", "percentiles"
                    ),
                    AggregateCurriedFunctionMapper(
                        "value", "histogram", "histogramMerge", "histogram_buckets"
                    ),
                    AggregateCurriedFunctionMapper(
                        "value", "histogramIf", "histogramMergeIf", "histogram_buckets"
                    ),
                ],
            ),
            subscription_processors=None,
            subscription_validators=None,
        )
