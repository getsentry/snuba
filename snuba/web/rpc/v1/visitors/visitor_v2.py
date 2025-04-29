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


class PrefixLabelsVisitor(TimeSeriesRequestVisitor):
    """
    This vistor prefixes all labels in an expression with an identifier unique to the expression its in.
    This is useful because it makes it so that if you have duplicate labels across different expressions,
    its ok and doesnt cause issues. It requires all top level expressions to have a unique label.

    example:
    expression {
        label: "expr_label"
        formula {
            column: "column_name"
        }
    }
    will be transformed into:
    expression {
        label: "expr_label"
        formula {
            column: "expr_0.column_name"
        }
    """

    def __init__(self) -> None:
        self.current_label_prefix = ""
        super().__init__()

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        # validate that all top level expressions have unique label
        seen_labels: set[str] = set()
        for expr in node.expressions:
            if expr.label == "":
                raise ValueError("Expression label is required")
            elif expr.label in seen_labels:
                raise ValueError(f"Duplicate expression label: {expr.label}")
            seen_labels.add(expr.label)

        # add prefix to all labels under the same expression
        for expr in node.expressions:
            self.current_label_prefix = expr.label
            # instead of visiting the expression directly, we visit one level down
            # what is inside of the expression. this is bc we dont want to modify the
            # top level expressions, but we do want to modify the labels of the inner expressions
            expr_type = expr.WhichOneof("expression")
            if expr_type is None:
                raise ValueError("Unknown expression type: None")
            self.visit(getattr(expr, expr_type))

    def visit_Expression(self, node: Expression) -> None:
        if node.label != "":
            node.label = f"{self.current_label_prefix}.{node.label}"
        expr_type = node.WhichOneof("expression")
        if expr_type is None:
            raise ValueError("Unknown expression type: None")
        self.visit(getattr(node, expr_type))

    def visit_AttributeAggregation(self, node: AttributeAggregation) -> None:
        if node.label != "":
            node.label = f"{self.current_label_prefix}.{node.label}"

    def visit_BinaryFormula(self, node: Expression.BinaryFormula) -> None:
        self.visit(node.left)
        self.visit(node.right)

    def visit_AttributeConditionalAggregation(
        self, node: AttributeConditionalAggregation
    ) -> None:
        if node.label != "":
            node.label = f"{self.current_label_prefix}.{node.label}"

    def visit_Literal(self, node: Literal) -> None:
        return
