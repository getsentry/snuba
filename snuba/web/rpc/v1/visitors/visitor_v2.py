from abc import ABC
from random import randint

from google.protobuf.message import Message
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.formula_pb2 import Literal
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeAggregation

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


class TimeSeriesRequestVisitor(ABC):
    def visit(self, node: Message) -> None:
        method_name = "visit_" + type(node).__name__
        visitor = getattr(self, method_name, self.generic_visit)
        visitor(node)

    def generic_visit(self, node: Message) -> None:
        raise NotImplementedError(f"No visit_{type(node).__name__} method")


class ValidateAliasVisitor(TimeSeriesRequestVisitor):
    """
    This visitor validates that all top level expression labels are unique.
    It also adds default labels to top-level expressions that dont have them.
    """

    def __init__(self) -> None:
        self.expression_labels: set[str] = set()
        super().__init__()

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        for expr in node.expressions:
            if expr.label == "":
                continue
            if expr.label in self.expression_labels:
                raise BadSnubaRPCRequestException(
                    f"Duplicate expression label: {expr.label}"
                )
            self.expression_labels.add(expr.label)

        for expr in node.expressions:
            if expr.label == "":
                new_label = f"expr_{randint(0, 1000000)}"
                while new_label in self.expression_labels:
                    # theoretically this could loop forever, but the probability is so small
                    new_label = f"expr_{randint(0, 1000000)}"
                expr.label = new_label
                self.expression_labels.add(new_label)


class RemoveInnerExpressionLabelsVisitor(TimeSeriesRequestVisitor):
    """
    Removes all labels inside expressions except for the top-level expression label.
    """

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        for expr in node.expressions:
            self.visit(expr)

    def visit_Expression(self, node: Expression) -> None:
        expr_type = node.WhichOneof("expression")
        if expr_type is None:
            raise ValueError("Unknown expression type: None")
        self.visit(getattr(node, expr_type))

    def visit_AttributeAggregation(self, node: AttributeAggregation) -> None:
        node.label = ""

    def visit_AttributeConditionalAggregation(
        self, node: AttributeConditionalAggregation
    ) -> None:
        node.label = ""

    def visit_BinaryFormula(self, node: Expression.BinaryFormula) -> None:
        self.visit(node.left)
        self.visit(node.right)

    def visit_Literal(self, node: Literal) -> None:
        return


class AddAggregateLabelsVisitor(TimeSeriesRequestVisitor):
    """
    Adds a label to aggregate and conditional aggregate expressions, that is the same as the label of
    the expression.
    """

    def __init__(self) -> None:
        self.current_label = ""
        super().__init__()

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        for expr in node.expressions:
            self.visit(expr)

    def visit_Expression(self, node: Expression) -> None:
        self.current_label = node.label
        expr_type = node.WhichOneof("expression")
        if expr_type == "aggregation" or expr_type == "conditional_aggregation":
            self.visit(getattr(node, expr_type))

    def visit_AttributeAggregation(self, node: AttributeAggregation) -> None:
        node.label = self.current_label

    def visit_AttributeConditionalAggregation(
        self, node: AttributeConditionalAggregation
    ) -> None:
        node.label = self.current_label


def preprocess_expression_labels(msg: TimeSeriesRequest) -> None:
    ValidateAliasVisitor().visit(msg)
    RemoveInnerExpressionLabelsVisitor().visit(msg)

    # We need this visitor because the endpoint only behaves correctly if
    # all aggregates have the exact same label as the expression they are in
    AddAggregateLabelsVisitor().visit(msg)
