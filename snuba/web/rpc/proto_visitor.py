from __future__ import annotations

from abc import ABC, abstractmethod
from types import MethodType
from typing import Any, Callable, Generic, TypeVar

from google.protobuf.message import Message as ProtobufMessage
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
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeAggregation
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

Tin = TypeVar("Tin", bound=ProtobufMessage)


class ProtoWrapper(Generic[Tin], ABC):
    def __init__(self, underlying_proto: Tin):
        self.underlying_proto = underlying_proto

    @abstractmethod
    def accept(self, visitor: ProtoVisitor) -> None:
        pass


class ColumnWrapper(ProtoWrapper[Column]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_ColumnWrapper(self)
        column = self.underlying_proto
        if column.HasField("formula"):
            ColumnWrapper(column.formula.left).accept(visitor)
            ColumnWrapper(column.formula.right).accept(visitor)
        if column.HasField("conditional_formula"):
            conditional = column.conditional_formula
            if conditional.HasField("condition"):
                if conditional.condition.HasField("left"):
                    ColumnWrapper(conditional.condition.left).accept(visitor)
                if conditional.condition.HasField("right"):
                    ColumnWrapper(conditional.condition.right).accept(visitor)
            # Note: 'match' is a Python keyword, so use getattr
            if conditional.HasField("match"):
                ColumnWrapper(getattr(conditional, "match")).accept(visitor)
            if conditional.HasField("default"):
                ColumnWrapper(conditional.default).accept(visitor)


class AggregationComparisonFilterWrapper(ProtoWrapper[AggregationComparisonFilter]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_AggregationComparisonFilterWrapper(self)


class AggregationFilterWrapper(ProtoWrapper[AggregationFilter]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_AggregationFilterWrapper(self)
        aggregation_filter = self.underlying_proto
        if aggregation_filter.HasField("comparison_filter"):
            AggregationComparisonFilterWrapper(aggregation_filter.comparison_filter).accept(visitor)

        if aggregation_filter.HasField("and_filter"):
            for agg_filter in aggregation_filter.and_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)

        if aggregation_filter.HasField("or_filter"):
            for agg_filter in aggregation_filter.or_filter.filters:
                AggregationFilterWrapper(agg_filter).accept(visitor)


class TimeSeriesExpressionWrapper(ProtoWrapper[TimeSeriesExpression]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TimeSeriesExpressionWrapper(self)

        time_series_expression = self.underlying_proto
        if time_series_expression.HasField("formula"):
            TimeSeriesExpressionWrapper(time_series_expression.formula.left).accept(visitor)
            TimeSeriesExpressionWrapper(time_series_expression.formula.right).accept(visitor)


class TraceItemTableRequestWrapper(ProtoWrapper[TraceItemTableRequest]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TraceItemTableRequestWrapper(self)
        trace_item_table_request = self.underlying_proto
        for col in trace_item_table_request.columns:
            ColumnWrapper(col).accept(visitor)
        for ob in trace_item_table_request.order_by:
            ColumnWrapper(ob.column).accept(visitor)
        if trace_item_table_request.HasField("aggregation_filter"):
            AggregationFilterWrapper(trace_item_table_request.aggregation_filter).accept(visitor)


class TimeSeriesRequestWrapper(ProtoWrapper[TimeSeriesRequest]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TimeSeriesRequestWrapper(self)
        time_series_request = self.underlying_proto
        for expression in time_series_request.expressions:
            TimeSeriesExpressionWrapper(expression).accept(visitor)


class TraceItemFilterWrapper(ProtoWrapper[TraceItemFilter]):
    def accept(self, visitor: ProtoVisitor) -> None:
        visitor.visit_TraceItemFilterWrapper(self)
        item_filter = self.underlying_proto
        filter_value = item_filter.WhichOneof("value")
        if filter_value in ("and_filter", "or_filter", "not_filter"):
            filters = getattr(item_filter, filter_value).filters
            for f in filters:
                TraceItemFilterWrapper(f).accept(visitor)


class ProtoVisitor(ABC):
    """
    Proto visitor design is split into two parts:
    1. the visitor. Responsible for only executing work on the object it is visiting
    2. the wrappers that wrap around the protobuf objects. Think of these as the "nodes" of an AST. Responsible for traversal

    I put the responsibility of traversing the wrapper structure onto the wrapper itself. This is a valid approach according
    to Design Patterns: Elements of Reusable Object-Oriented Software page 339: "We can
    put responsibility for traversal in any of three places: in the object structure,
    in the visitor, or in a separate iterator object (see Iterator (257)).
    Often the object structure is responsible for iteration. A collection will simply
    iterate over its elements, calling the Accept operation on each. A composite
    will commonly traverse itself by having each Accept operation traverse the
    element's children and call Accept on each of them recursively."

    If the visitor is responsible for traversal, then we'd have to implement all the visit methods for each protobuf field,
    along with the corresponding wrapper class. There are too many in our codebase. By delegating the traversal
    responsibility to the wrapper, we only need to create the wrapper class and the corresponding visit method when we need it.

    How to use:
    1. Given the protobuf message, split its fields into the ones you care about visiting and doing work on, vs the ones
    you do not care about
    2. For the fields you want to do work on, implement the visit methods. ex) def visit_ColumnWrapper, def visit_AggregationComparisonFilterWrapper,
    these visit methods will not do any traversal. It will simply execute work
    3. Implement the corresponding wrapper classes ex) class ColumnWrapper, class AggregationComparisonFilterWrapper
    4. The wrapper classes' accept methods will call the matching visit method and traverse itself
    5. For fields that you do not want to do work on, calling vistor.visit_IrrelevantField will default to the do_nothing
    method below


    """

    "Proto visitor design is split into two parts"

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
    Originally, the protobuf request did not support conditional aggregations. When they were added, the file structure did not allow adding a TraceItemFilter to the AttributeAggregation message without breaking backwards compatibility. A new AttributeConditionalAggregation message was added whose fields are a superset of of AttributeAggregation. AttributeAggregation was deprecated.

    This code transforms every AttributeAggregation to an AttributeConditionalAggregation to be able to serve queries before the deprecation, and have the rest of the query pipeline only concerned with one kind of aggregation object.
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


class ContainsAggregateVisitor(ProtoVisitor):
    def __init__(self) -> None:
        super().__init__()
        self.contains_aggregate = False

    def visit_ColumnWrapper(self, column_wrapper: ColumnWrapper) -> None:
        if column_wrapper.underlying_proto.HasField("conditional_aggregation"):
            self.contains_aggregate = True


class GetExpressionAggregationsVisitor(ProtoVisitor):
    """
    is mean to visit a timeseries expression object and return a list of the aggregations in it
    """

    def __init__(self) -> None:
        super().__init__()
        self.aggregations: list[AttributeAggregation | AttributeConditionalAggregation] = []

    def visit_TimeSeriesExpressionWrapper(
        self, expression_wrapper: TimeSeriesExpressionWrapper
    ) -> None:
        if expression_wrapper.underlying_proto.HasField("aggregation"):
            self.aggregations.append(expression_wrapper.underlying_proto.aggregation)
        elif expression_wrapper.underlying_proto.HasField("conditional_aggregation"):
            self.aggregations.append(expression_wrapper.underlying_proto.conditional_aggregation)
