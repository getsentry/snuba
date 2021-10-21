from abc import ABC
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Tuple, Union

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
from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    CurriedFunctionCallMapper,
    FunctionCallMapper,
)
from snuba.clickhouse.translators.snuba.mappers import (
    FunctionNameMapper,
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
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.processors.granularity_processor import GranularityProcessor
from snuba.query.processors.object_id_rate_limiter import (
    OrganizationRateLimiterProcessor,
    ProjectRateLimiterProcessor,
)
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.validation.validators import (
    EntityRequiredColumnValidator,
    GranularityValidator,
)
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
            validators=[
                EntityRequiredColumnValidator({"org_id", "project_id"}),
                GranularityValidator(minimum=10),
            ],
            required_time_column="timestamp",
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            GranularityProcessor(),
            TimeSeriesProcessor({"bucketed_time": "timestamp"}, ("timestamp",)),
            OrganizationRateLimiterProcessor(org_column="org_id"),
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
                functions=[
                    FunctionNameMapper("uniq", "uniqCombined64Merge"),
                    FunctionNameMapper("uniqIf", "uniqCombined64MergeIf"),
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
                functions=[
                    FunctionNameMapper("sum", "sumMerge"),
                    FunctionNameMapper("sumIf", "sumMergeIf"),
                ],
            ),
        )


def _build_parameters(
    expression: Union[FunctionCall, CurriedFunctionCall],
    children_translator: SnubaClickhouseStrictTranslator,
    aggregated_col_name: str,
) -> Tuple[Expression, ...]:
    assert isinstance(expression.parameters[0], ColumnExpr)
    return (
        ColumnExpr(None, expression.parameters[0].table_name, aggregated_col_name),
        *[p.accept(children_translator) for p in expression.parameters[1:]],
    )


def _should_transform_aggregation(
    function_name: str,
    expected_function_name: str,
    column_to_map: str,
    function_call: Union[FunctionCall, CurriedFunctionCall],
) -> bool:
    return (
        function_name == expected_function_name
        and len(function_call.parameters) > 0
        and isinstance(function_call.parameters[0], ColumnExpr)
        and function_call.parameters[0].column_name == column_to_map
    )


@dataclass(frozen=True)
class AggregateFunctionMapper(FunctionCallMapper):
    """
    Turns expressions like max(value) into maxMerge(max)
    or maxIf(value, condition) into maxMergeIf(max, condition)
    """

    column_to_map: str
    from_name: str
    to_name: str
    aggr_col_name: str

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if not _should_transform_aggregation(
            expression.function_name, self.from_name, self.column_to_map, expression
        ):
            return None

        return FunctionCall(
            expression.alias,
            self.to_name,
            _build_parameters(expression, children_translator, self.aggr_col_name),
        )


@dataclass(frozen=True)
class AggregateCurriedFunctionMapper(CurriedFunctionCallMapper):
    """
    Turns expressions like quantiles(0.9)(value) into quantilesMerge(0.9)(percentiles)
    or quantilesIf(0.9)(value, condition) into quantilesMergeIf(0.9)(percentiles, condition)
    """

    column_to_map: str
    from_name: str
    to_name: str
    aggr_col_name: str

    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[CurriedFunctionCall]:
        if not _should_transform_aggregation(
            expression.internal_function.function_name,
            self.from_name,
            self.column_to_map,
            expression,
        ):
            return None

        return CurriedFunctionCall(
            expression.alias,
            FunctionCall(
                None,
                self.to_name,
                tuple(
                    p.accept(children_translator)
                    for p in expression.internal_function.parameters
                ),
            ),
            _build_parameters(expression, children_translator, self.aggr_col_name),
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
                ],
            ),
        )
