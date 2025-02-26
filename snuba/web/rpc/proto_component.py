from __future__ import annotations

from abc import ABC, abstractmethod
from types import MethodType
from typing import Any, Callable

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression as TimeSeriesExpression,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    Column,
)


class ProtoWrapper(ABC):
    def __init__(self, underlying_proto: Any):
        self.underlying_proto = underlying_proto

    @abstractmethod
    def accept(self, visitor: ProtoVisitor) -> None:
        pass


class ColumnWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_ColumnWrapper(self)
        column = self.underlying_proto
        if column.HasField("formula"):
            ColumnWrapper(column.formula.left).accept(visitor)
            ColumnWrapper(column.formula.right).accept(visitor)


class AggregationComparisonFilterWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_AggregationComparisonFilterWrapper(self)


class AggregationFilterWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_AggregationFilterWrapper(self)
        aggregation_filter = self.underlying_proto
        if aggregation_filter.HasField("comparison_filter"):
            AggregationComparisonFilterWrapper(
                aggregation_filter.comparison_filter
            ).accept(visitor)

        if aggregation_filter.HasField("and_filter"):
            for agg_filter in aggregation_filter.and_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)

        if aggregation_filter.HasField("or_filter"):
            for agg_filter in aggregation_filter.or_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)


class TimeSeriesExpressionWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TimeSeriesExpressionWrapper(self)

        time_series_expression = self.underlying_proto
        if time_series_expression.HasField("formula"):
            TimeSeriesExpressionWrapper(time_series_expression.formula.left).accept(
                visitor
            )
            TimeSeriesExpressionWrapper(time_series_expression.formula.right).accept(
                visitor
            )


class TraceItemTableRequestWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TraceItemTableRequestWrapper(self)
        trace_item_table_request = self.underlying_proto
        for col in trace_item_table_request.columns:
            ColumnWrapper(col).accept(visitor)
        for ob in trace_item_table_request.order_by:
            ColumnWrapper(ob.column).accept(visitor)
        if trace_item_table_request.HasField("aggregation_filter"):
            AggregationFilterWrapper(
                trace_item_table_request.aggregation_filter
            ).accept(visitor)


class TimeSeriesRequestWrapper(ProtoWrapper):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TimeSeriesRequestWrapper(self)
        time_series_request = self.underlying_proto
        for expression in time_series_request.expressions:
            TimeSeriesExpressionWrapper(expression).accept(visitor)


class ProtoVisitor(ABC):
    def __getattr__(self, visit_method: str) -> Callable[..., Any]:
        subclass_visit_method = getattr(self.__class__, visit_method, None)
        if callable(subclass_visit_method):
            return MethodType(subclass_visit_method, self)

        def do_nothing(*args: Any, **kwargs: Any) -> None:
            pass

        return do_nothing


def _convert_aggregation_to_conditional_aggregation(
    input: Column | AggregationComparisonFilter | TimeSeriesExpression,
) -> None:
    if input.HasField("aggregation"):
        aggregation = input.aggregation
        input.ClearField("aggregation")
        input.conditional_aggregation.CopyFrom(
            AttributeConditionalAggregation(
                aggregate=aggregation.aggregate,
                key=aggregation.key,
                label=aggregation.label,
                extrapolation_mode=aggregation.extrapolation_mode,
            )
        )


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

    def visit_ColumnWrapper(self, column_wrapper: ColumnWrapper) -> None:
        _convert_aggregation_to_conditional_aggregation(column_wrapper.underlying_proto)

    def visit_AggregationComparisonFilterWrapper(
        self, aggregation_comparison_filter_wrapper: AggregationComparisonFilterWrapper
    ) -> None:
        _convert_aggregation_to_conditional_aggregation(
            aggregation_comparison_filter_wrapper.underlying_proto
        )

    def visit_TimeSeriesExpressionWrapper(
        self, time_series_expression_wrapper: TimeSeriesExpressionWrapper
    ) -> None:
        _convert_aggregation_to_conditional_aggregation(
            time_series_expression_wrapper.underlying_proto
        )
