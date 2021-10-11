from abc import ABC
from typing import Mapping, Sequence

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
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToCurriedFunction,
    ColumnToFunction,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.pipeline.simple_pipeline import SimplePipelineBuilder
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.granularity_processor import GranularityProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import EntityRequiredColumnValidator
from snuba.request.request_settings import RequestSettings


class TagsTypeTransformer(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            if not isinstance(exp, SubscriptableReference):
                return exp

            key = exp.key
            if not isinstance(key.value, str) or not key.value.isdigit():
                raise InvalidExpressionException.from_args(
                    exp,
                    "Expected a string key containing an integer in subscriptable.",
                )

            return SubscriptableReference(
                exp.alias, exp.column, Literal(None, int(key.value))
            )

        query.transform_expressions(transform_expression)


class MetricsEntity(Entity, ABC):
    def __init__(
        self,
        writable_storage_key: StorageKey,
        readable_storage_key: StorageKey,
        value_schema: Sequence[Column[SchemaModifiers]],
        mappers: TranslationMappers,
    ) -> None:
        writable_storage = get_writable_storage(writable_storage_key)
        readable_storage = get_storage(readable_storage_key)

        super().__init__(
            storages=[writable_storage, readable_storage],
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
            abstract_column_set=ColumnSet(
                [
                    Column("org_id", UInt(64)),
                    Column("project_id", UInt(64)),
                    Column("metric_id", UInt(64)),
                    Column("timestamp", DateTime()),
                    Column("tags", Nested([("key", UInt(64)), ("value", UInt(64))])),
                    *value_schema,
                ]
            ),
            join_relationships={},
            writable_storage=writable_storage,
            validators=[EntityRequiredColumnValidator({"org_id", "project_id"})],
            required_time_column="timestamp",
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            GranularityProcessor(),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            ProjectRateLimiterProcessor(project_column="project_id"),
            TagsTypeTransformer(),
        ]


class MetricsSetsEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_BUCKETS,
            readable_storage_key=StorageKey.METRICS_SETS,
            value_schema=[
                Column("value", AggregateFunction("uniqCombined64", [UInt(64)])),
            ],
            mappers=TranslationMappers(
                columns=[
                    ColumnToFunction(
                        None,
                        "value",
                        "uniqCombined64Merge",
                        (ColumnExpr(None, None, "value"),),
                    ),
                ],
            ),
        )


class MetricsCountersEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_COUNTERS_BUCKETS,
            readable_storage_key=StorageKey.METRICS_COUNTERS,
            value_schema=[Column("value", AggregateFunction("sum", [Float(64)]))],
            mappers=TranslationMappers(
                columns=[
                    ColumnToFunction(
                        None, "value", "sumMerge", (ColumnExpr(None, None, "value"),),
                    ),
                ],
            ),
        )


def merge_mapper(name: str) -> ColumnToFunction:
    return ColumnToFunction(
        None, name, f"{name}Merge", (ColumnExpr(None, None, name),),
    )


class MetricsDistributionsEntity(MetricsEntity):
    def __init__(self) -> None:
        super().__init__(
            writable_storage_key=StorageKey.METRICS_DISTRIBUTIONS_BUCKETS,
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
            ],
            mappers=TranslationMappers(
                columns=[
                    ColumnToCurriedFunction(
                        None,
                        "percentiles",
                        FunctionCall(
                            None,
                            "quantilesMerge",
                            tuple(
                                Literal(None, quant)
                                for quant in [0.5, 0.75, 0.9, 0.95, 0.99]
                            ),
                        ),
                        (ColumnExpr(None, None, "percentiles"),),
                    ),
                    merge_mapper("min"),
                    merge_mapper("max"),
                    merge_mapper("avg"),
                    merge_mapper("sum"),
                    merge_mapper("count"),
                ],
            ),
        )
