from random import randint

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.formula_pb2 import Literal
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
)

from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.visitors.visitor_v2 import RequestVisitor


class ValidateColumnLabelsVisitor(RequestVisitor):
    """
    Make sure all top level columns have unique labels.
    It also adds default labels to any column that doesnt have one.
    """

    def __init__(self) -> None:
        self.column_labels: set[str] = set()
        super().__init__()

    def visit_TraceItemTableRequest(self, node: TraceItemTableRequest) -> None:
        for column in node.columns:
            if column.label == "":
                continue
            if column.label in self.column_labels:
                raise BadSnubaRPCRequestException(f"Duplicate column label: {column.label}")
            self.column_labels.add(column.label)

        for column in node.columns:
            if column.label == "":
                new_label = f"column_{randint(0, 1000000)}"
                while new_label in self.column_labels:
                    # theoretically this could loop forever, but the probability is so small
                    new_label = f"expr_{randint(0, 1000000)}"
                column.label = new_label
                self.column_labels.add(new_label)


class SetColumnLabelsVisitor(RequestVisitor):
    """
    Adds default labels to any column that doesnt have one.
    """

    def __init__(self) -> None:
        self.column_labels: set[str] = set()
        super().__init__()

    def visit_TraceItemTableRequest(self, node: TraceItemTableRequest) -> None:
        # collect the existing labels so I dont generate duplicates
        for column in node.columns:
            if column.label == "":
                continue
            self.column_labels.add(column.label)

        # add default labels to any column that doesnt have one
        for column in node.columns:
            if column.label == "":
                new_label = f"column_{randint(0, 1000000)}"
                while new_label in self.column_labels:
                    # theoretically this could loop forever, but the probability is so small
                    new_label = f"expr_{randint(0, 1000000)}"
                column.label = new_label
                self.column_labels.add(new_label)


class NormalizeFormulaLabelsVisitor(RequestVisitor):
    """
    Sets the labels of a formula such that the top-level label of the column is unchanged,
    and each left and right part becomes top_level_label.left and top_level_label.right.

    ex if we had:
    myformula = (10 + 20) / 30
    the overall formula label would still be myformula,
    (10 + 20) would get the label myformula.left,
    30 would get the label myformula.right,
    10 would get the label myformula.left.left,
    20 would get the label myformula.left.right,
    """

    def visit_TimeSeriesRequest(self, node: TimeSeriesRequest) -> None:
        for expr in node.expressions:
            if expr.WhichOneof("expression") == "formula":
                self.visit(expr.formula.left, f"{expr.label}.left")
                self.visit(expr.formula.right, f"{expr.label}.right")

    def visit_Expression(self, node: Expression, new_label: str) -> None:
        node.label = new_label
        whichone = node.WhichOneof("expression")
        assert whichone is not None
        self.visit(getattr(node, whichone), new_label)

    def visit_TraceItemTableRequest(self, node: TraceItemTableRequest) -> None:
        for column in node.columns:
            if column.WhichOneof("column") == "formula":
                self.visit(column.formula.left, f"{column.label}.left")
                self.visit(column.formula.right, f"{column.label}.right")

    def visit_Column(self, node: Column, new_label: str) -> None:
        node.label = new_label

        column_type = node.WhichOneof("column")
        # Handle columns that only have a label but no column type
        # (e.g., order_by columns that reference existing columns by label)
        if column_type is None:
            return

        match column_type:
            case "key":
                self.visit(node.key, new_label)
            case "aggregation":
                self.visit(node.aggregation, new_label)
            case "conditional_aggregation":
                self.visit(node.conditional_aggregation, new_label)
            case "formula":
                self.visit(node.formula, new_label)
            case "literal":
                self.visit(node.literal, new_label)
            case _:
                raise ValueError(f"Unknown column type: {column_type}")

    def visit_AttributeKey(self, node: AttributeKey, new_label: str) -> None:
        return

    def visit_AttributeAggregation(self, node: AttributeAggregation, new_label: str) -> None:
        node.label = new_label

    def visit_AttributeConditionalAggregation(
        self, node: AttributeConditionalAggregation, new_label: str
    ) -> None:
        node.label = new_label

    def visit_BinaryFormula(self, node: Column.BinaryFormula, new_label: str) -> None:
        self.visit(node.left, f"{new_label}.left")
        self.visit(node.right, f"{new_label}.right")

    def visit_Literal(self, node: Literal, new_label: str) -> None:
        return


class SetAggregateLabelsVisitor(RequestVisitor):
    """
    Sets the label of all aggregates in columns to be the same as the label of
    the column. This is required for correct behavior of the endpoint.
    (When I say aggregates in columns I mean columns that directly have an aggregation or conditional aggregation)

    Ex:
    Column:
        Aggregation:
            Label: "myagg"
            function: SUM
            Key: "kyles_measurement"
        Label: "mycolumn"

    After running this visitor:
    Column:
        Aggregation:
            Label: "mycolumn"
            function: SUM
            Key: "kyles_measurement"
        Label: "mycolumn"
    """

    def __init__(self) -> None:
        self.current_label = ""
        super().__init__()

    def visit_TraceItemTableRequest(self, node: TraceItemTableRequest) -> None:
        for expr in node.columns:
            self.visit(expr)

    def visit_Column(self, node: Column) -> None:
        self.current_label = node.label
        col_type = node.WhichOneof("column")
        if col_type == "aggregation" or col_type == "conditional_aggregation":
            self.visit(getattr(node, col_type))

    def visit_AttributeAggregation(self, node: AttributeAggregation) -> None:
        node.label = self.current_label

    def visit_AttributeConditionalAggregation(self, node: AttributeConditionalAggregation) -> None:
        node.label = self.current_label
