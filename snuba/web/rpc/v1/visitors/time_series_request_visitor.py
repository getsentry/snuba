from random import randint

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.formula_pb2 import Literal
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba.state import get_config
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.visitors.trace_item_table_request_visitor import (
    NormalizeFormulaLabelsVisitor,
)
from snuba.web.rpc.v1.visitors.visitor_v2 import RequestVisitor


class ValidateAliasVisitor(RequestVisitor):
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
                raise BadSnubaRPCRequestException(f"Duplicate expression label: {expr.label}")
            self.expression_labels.add(expr.label)

        for expr in node.expressions:
            if expr.label == "":
                new_label = f"expr_{randint(0, 1000000)}"
                while new_label in self.expression_labels:
                    # theoretically this could loop forever, but the probability is so small
                    new_label = f"expr_{randint(0, 1000000)}"
                expr.label = new_label
                self.expression_labels.add(new_label)


class RemoveInnerExpressionLabelsVisitor(RequestVisitor):
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

    def visit_AttributeConditionalAggregation(self, node: AttributeConditionalAggregation) -> None:
        node.label = ""

    def visit_BinaryFormula(self, node: Expression.BinaryFormula) -> None:
        self.visit(node.left)
        self.visit(node.right)

    def visit_Literal(self, node: Literal) -> None:
        return


class AddAggregateLabelsVisitor(RequestVisitor):
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

    def visit_AttributeConditionalAggregation(self, node: AttributeConditionalAggregation) -> None:
        node.label = self.current_label


class RejectTimestampAsStringVisitor(RequestVisitor):
    def visit_TraceItemTableRequest(self, node: TraceItemTableRequest) -> None:
        self.visit(node.filter)

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        self.visit(node.filter)

    def visit_TraceItemFilter(self, node: TraceItemFilter) -> None:
        if node.HasField("and_filter"):
            for f in node.and_filter.filters:
                self.visit(f)
        elif node.HasField("or_filter"):
            for f in node.or_filter.filters:
                self.visit(f)
        elif node.HasField("not_filter"):
            for f in node.not_filter.filters:
                self.visit(f)
        elif node.HasField("comparison_filter"):
            k = node.comparison_filter.key
            if k.name == "sentry.timestamp" and k.type == AttributeKey.TYPE_STRING:
                if get_config("eap.reject_string_timestamp_filters", 1):
                    raise BadSnubaRPCRequestException(
                        "sentry.timestamp can only be compared to TYPE_INT or TYPE_DOUBLE, got TYPE_STRING"
                    )


class GetSubformulaLabelsVisitor(RequestVisitor):
    """
    given a formula, returns a list of the expected labels for the leaf nodes (non-formula nodes)
    of the formula:
    ex: for (a+b)+c it would be [myformlabel.right, myformlabel.left.left, formula.left.right...]
    """

    def __init__(self) -> None:
        self.labels: dict[str, list[str]] = {}
        self.curr_formula: str = ""
        super().__init__()

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        for expr in node.expressions:
            if expr.WhichOneof("expression") == "formula":
                self.curr_formula = expr.label
                self.labels[self.curr_formula] = []
                self.visit(expr.formula, expr.label)
                if self.labels[self.curr_formula] == []:
                    del self.labels[self.curr_formula]

    def visit_BinaryFormula(self, node: Expression.BinaryFormula, curr_label: str = "") -> None:
        self.visit(node.left, curr_label + ".left")
        self.visit(node.right, curr_label + ".right")

    def visit_Expression(self, node: Expression, curr_label: str = "") -> None:
        expr_type = node.WhichOneof("expression")
        assert expr_type is not None
        self.visit(getattr(node, expr_type), curr_label)

    def visit_AttributeAggregation(self, node: AttributeAggregation, curr_label: str = "") -> None:
        self.labels[self.curr_formula].append(curr_label)

    def visit_AttributeConditionalAggregation(
        self, node: AttributeConditionalAggregation, curr_label: str = ""
    ) -> None:
        self.labels[self.curr_formula].append(curr_label)

    def visit_Literal(self, node: Literal, curr_label: str = "") -> None:
        return


def preprocess_expression_labels(msg: TimeSeriesRequest) -> None:
    RejectTimestampAsStringVisitor().visit(msg)
    ValidateAliasVisitor().visit(msg)
    RemoveInnerExpressionLabelsVisitor().visit(msg)

    # We need this visitor because the endpoint only behaves correctly if
    # all aggregates have the exact same label as the expression they are in
    AddAggregateLabelsVisitor().visit(msg)
    NormalizeFormulaLabelsVisitor().visit(msg)
