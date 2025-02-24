from __future__ import annotations

from abc import ABC, abstractmethod

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    Column,
    TraceItemTableRequest,
)


class ProtoWrapper(ABC):
    @abstractmethod
    def accept(self, visitor: ProtoVisitor) -> None:
        pass


class ColumnWrapper(ProtoWrapper):
    def __init__(self, column: Column):
        self.column = column

    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit(self)

        if self.column.HasField("formula"):
            ColumnWrapper(self.column.formula.left).accept(visitor)
            ColumnWrapper(self.column.formula.right).accept(visitor)


class AggregationComparisonFilterWrapper(ProtoWrapper):
    def __init__(self, aggregation_comparison_filter: AggregationComparisonFilter):
        self.aggregation_comparison_filter = aggregation_comparison_filter

    def accept(self, visitor):
        visitor.visit(self)


class AggregationFilterWrapper(ProtoWrapper):
    def __init__(self, aggregation_filter: AggregationFilter):
        self.aggregation_filter = aggregation_filter

    def accept(self, visitor: ProtoVisitor) -> None:
        if self.aggregation_filter.HasField("comparison_filter"):
            AggregationComparisonFilterWrapper(
                self.aggregation_filter.comparison_filter
            ).accept(visitor)

        if self.aggregation_filter.HasField("and_filter"):
            for agg_filter in self.aggregation_filter.and_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)

        if self.aggregation_filter.HasField("or_filter"):
            for agg_filter in self.aggregation_filter.or_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)


class TimeSeriesExpressionWrapper(ProtoWrapper):
    def __init__(self, time_series_expression: TimeSeriesExpression):
        self.time_series_expression = time_series_expression

    def accept(self, visitor: ProtoVisitor) -> None:
        if self.time_series_expression.HasField("aggregation"):
            visitor.visit(TimeSeriesExpressionWrapper(self.time_series_expression))

        if self.time_series_expression.HasField("formula"):
            TimeSeriesExpressionWrapper(
                self.time_series_expression.formula.left
            ).accept(visitor)
            TimeSeriesExpressionWrapper(
                self.time_series_expression.formula.right
            ).accept(visitor)


class TraceItemTableRequestWrapper(ProtoWrapper):
    def __init__(self, trace_item_table_request: TraceItemTableRequest):
        self.trace_item_table_request = trace_item_table_request

    def accept(self, visitor: ProtoVisitor) -> None:
        for col in self.trace_item_table_request.columns:
            ColumnWrapper(col).accept(visitor)
        for ob in self.trace_item_table_request.order_by:
            ColumnWrapper(ob.column).accept(visitor)
        if self.trace_item_table_request.HasField("aggregation_filter"):
            AggregationFilterWrapper(
                self.trace_item_table_request.aggregation_filter
            ).accept(visitor)


class TimeSeriesRequestWrapper(ProtoWrapper):
    def __init__(self, time_series_request: TimeSeriesRequest):
        self.time_series_request = time_series_request

    def accept(self, visitor: ProtoVisitor) -> None:
        for expression in self.time_series_request.expressions:
            TimeSeriesExpressionWrapper(expression).accept(visitor)


class ProtoVisitor(ABC):
    @abstractmethod
    def visit(self, exp: ProtoWrapper) -> None:
        pass


class AggregationToConditionalAggregationVisitor(ProtoVisitor):
    """
    We support aggregation, but now we want to support conditional aggregation for the insights team, which only aggregates
    if the field satisfies the condition: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators#-if.
    For messages that don't have conditional aggregation, this function replaces the aggregation with a conditional aggregation,
    where the filter is null, and every field is the same. This allows code elsewhere to set the default condition to always
    be true.
    The reason we do this "transformation" is to avoid code fragmentation down the line, where we constantly have to check
    if the request contains `AttributeAggregation` or `AttributeConditionalAggregation`
    """

    def visit(
        self,
        input: ProtoWrapper,
    ) -> None:
        proto: Column | AggregationComparisonFilter | TimeSeriesExpression = None  # type: ignore
        if isinstance(input, ColumnWrapper):
            proto = input.column

        if isinstance(input, AggregationComparisonFilterWrapper):
            proto = input.aggregation_comparison_filter

        if isinstance(input, TimeSeriesExpressionWrapper):
            proto = input.time_series_expression

        if proto.HasField("aggregation"):
            aggregation = proto.aggregation
            proto.ClearField("aggregation")
            proto.conditional_aggregation.CopyFrom(
                AttributeConditionalAggregation(
                    aggregate=aggregation.aggregate,
                    key=aggregation.key,
                    label=aggregation.label,
                    extrapolation_mode=aggregation.extrapolation_mode,
                )
            )
