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
        visitor.visit_column(self)


class AggregationFilterWrapper(ProtoWrapper):
    def __init__(self, aggregation_filter: AggregationFilter):
        self.aggregation_filter = aggregation_filter

    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_aggregation_filter(self)


class TimeSeriesExpressionWrapper(ProtoWrapper):
    def __init__(self, time_series_expression: TimeSeriesExpression):
        self.time_series_expression = time_series_expression

    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_time_series_expression(self)


class TraceItemTableRequestWrapper(ProtoWrapper):
    def __init__(self, trace_item_table_request: TraceItemTableRequest):
        self.trace_item_table_request = trace_item_table_request

    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_trace_item_table_request(self)


class TimeSeriesRequestWrapper(ProtoWrapper):
    def __init__(self, time_series_request: TimeSeriesRequest):
        self.time_series_request = time_series_request

    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_time_series_request(self)


class ProtoVisitor(ABC):
    @abstractmethod
    def visit_column(self, exp: ColumnWrapper) -> None:
        pass

    @abstractmethod
    def visit_aggregation_filter(self, exp: AggregationFilterWrapper) -> None:
        pass

    @abstractmethod
    def visit_time_series_expression(self, exp: TimeSeriesExpressionWrapper) -> None:
        pass

    @abstractmethod
    def visit_trace_item_table_request(self, exp: TraceItemTableRequestWrapper) -> None:
        pass

    @abstractmethod
    def visit_time_series_request(self, exp: TimeSeriesRequestWrapper) -> None:
        pass


def _convert_aggregation_to_conditional_aggregation(
    input: Column | AggregationComparisonFilter | TimeSeriesExpression,
) -> None:
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
    def visit_time_series_request(self, exp: TimeSeriesRequestWrapper) -> None:
        time_series_request = exp.time_series_request
        for expression in time_series_request.expressions:
            self.visit_time_series_expression(TimeSeriesExpressionWrapper(expression))

    def visit_column(self, exp: ColumnWrapper) -> None:
        column = exp.column
        if column.HasField("aggregation"):
            _convert_aggregation_to_conditional_aggregation(column)

        if column.HasField("formula"):
            self.visit_column(ColumnWrapper(column.formula.left))
            self.visit_column(ColumnWrapper(column.formula.right))

    def visit_aggregation_filter(self, exp: AggregationFilterWrapper) -> None:
        aggregation_filter = exp.aggregation_filter
        if aggregation_filter.HasField("comparison_filter"):
            if aggregation_filter.comparison_filter.HasField("aggregation"):
                _convert_aggregation_to_conditional_aggregation(
                    aggregation_filter.comparison_filter
                )

        if aggregation_filter.HasField("and_filter"):
            for agg_filter in aggregation_filter.and_filter.filters:
                self.visit_aggregation_filter(AggregationFilterWrapper(agg_filter))

        if aggregation_filter.HasField("or_filter"):
            for agg_filter in aggregation_filter.or_filter.filters:
                self.visit_aggregation_filter(AggregationFilterWrapper(agg_filter))

    def visit_time_series_expression(self, exp: TimeSeriesExpressionWrapper) -> None:
        time_series_expression = exp.time_series_expression
        if time_series_expression.HasField("aggregation"):
            _convert_aggregation_to_conditional_aggregation(time_series_expression)

        if time_series_expression.HasField("formula"):
            self.visit_time_series_expression(
                TimeSeriesExpressionWrapper(time_series_expression.formula.left)
            )
            self.visit_time_series_expression(
                TimeSeriesExpressionWrapper(time_series_expression.formula.right)
            )

    def visit_trace_item_table_request(self, exp: TraceItemTableRequestWrapper) -> None:
        trace_item_table_request = exp.trace_item_table_request
        for col in trace_item_table_request.columns:
            self.visit_column(ColumnWrapper(col))
        for ob in trace_item_table_request.order_by:
            self.visit_column(ColumnWrapper(ob.column))
        if trace_item_table_request.HasField("aggregation_filter"):
            self.visit_aggregation_filter(
                AggregationFilterWrapper(trace_item_table_request.aggregation_filter)
            )
