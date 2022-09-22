from abc import ABC
from typing import Optional, Sequence

from snuba.clickhouse.columns import (
    AggregateFunction,
    Column,
    ColumnSet,
    DateTime,
    Nested,
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
from snuba.datasets.plans.partitioned_storage import (
    ColumnBasedStoragePartitionSelector,
    PartitionedStorageQueryPlanBuilder,
)
from snuba.datasets.storage import ReadableTableStorage, WritableTableStorage
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.processors.logical.granularity_processor import (
    DEFAULT_MAPPED_GRANULARITY_ENUM,
    PERFORMANCE_GRANULARITIES,
    MappedGranularityProcessor,
)
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
    QueryValidator,
)
from snuba.utils.schemas import Float


class GenericMetricsEntity(Entity, ABC):
    DEFAULT_COLUMNS = ColumnSet(
        [
            Column("org_id", UInt(64)),
            Column("project_id", UInt(64)),
            Column("metric_id", UInt(64)),
            Column("timestamp", DateTime()),
            Column("bucketed_time", DateTime()),
            Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
        ]
    )

    def __init__(
        self,
        readable_storage: ReadableTableStorage,
        writable_storage: Optional[WritableTableStorage],
        value_schema: ColumnSet,
        mappers: TranslationMappers,
        validators: Optional[Sequence[QueryValidator]] = None,
    ) -> None:
        storages = [readable_storage]
        if writable_storage:
            storages.append(writable_storage)

        if validators is None:
            validators = [EntityRequiredColumnValidator({"org_id", "project_id"})]

        super().__init__(
            storages=storages,
            query_pipeline_builder=SimplePipelineBuilder(
                query_plan_builder=PartitionedStorageQueryPlanBuilder(
                    storage=readable_storage,
                    storage_partition_selector=ColumnBasedStoragePartitionSelector(
                        readable_storage,
                        "org_id",
                    ),
                    mappers=TranslationMappers(
                        subscriptables=[
                            SubscriptableMapper(
                                from_column_table=None,
                                from_column_name="tags_raw",
                                to_nested_col_table=None,
                                to_nested_col_name="tags",
                                value_subcolumn_name="raw_value",
                            ),
                            SubscriptableMapper(
                                from_column_table=None,
                                from_column_name="tags",
                                to_nested_col_table=None,
                                to_nested_col_name="tags",
                                value_subcolumn_name="indexed_value",
                            ),
                        ],
                    ).concat(mappers),
                )
            ),
            abstract_column_set=(self.DEFAULT_COLUMNS + value_schema),
            join_relationships={},
            writable_storage=writable_storage,
            validators=validators,
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[LogicalQueryProcessor]:
        return [
            TagsTypeTransformer(),
            MappedGranularityProcessor(
                accepted_granularities=PERFORMANCE_GRANULARITIES,
                default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
            ),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            ReferrerRateLimiterProcessor(),
            OrganizationRateLimiterProcessor(org_column="org_id"),
            ProjectReferrerRateLimiter("project_id"),
            ProjectRateLimiterProcessor(project_column="project_id"),
            ResourceQuotaProcessor("project_id"),
        ]


class GenericMetricsSetsEntity(GenericMetricsEntity):
    READABLE_STORAGE = get_storage(StorageKey.GENERIC_METRICS_SETS)
    WRITABLE_STORAGE = get_storage(StorageKey.GENERIC_METRICS_SETS_RAW)

    def __init__(self) -> None:
        assert isinstance(self.WRITABLE_STORAGE, WritableTableStorage)
        super().__init__(
            readable_storage=self.READABLE_STORAGE,
            writable_storage=self.WRITABLE_STORAGE,
            value_schema=ColumnSet(
                [
                    Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
                ]
            ),
            validators=[EntityRequiredColumnValidator({"org_id", "project_id"})],
            mappers=TranslationMappers(
                functions=[
                    FunctionNameMapper("uniq", "uniqCombined64Merge"),
                    FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
                ],
            ),
        )


class GenericMetricsDistributionsEntity(GenericMetricsEntity):
    READABLE_STORAGE = get_storage(StorageKey.GENERIC_METRICS_DISTRIBUTIONS)
    WRITABLE_STORAGE = get_storage(StorageKey.GENERIC_METRICS_DISTRIBUTIONS_RAW)

    def __init__(self) -> None:
        assert isinstance(self.WRITABLE_STORAGE, WritableTableStorage)
        super().__init__(
            readable_storage=self.READABLE_STORAGE,
            writable_storage=self.WRITABLE_STORAGE,
            validators=[EntityRequiredColumnValidator({"org_id", "project_id"})],
            value_schema=ColumnSet(
                [
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
                ]
            ),
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
        )
