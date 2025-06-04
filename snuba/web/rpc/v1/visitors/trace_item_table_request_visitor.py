from abc import ABC

from google.protobuf.message import Message
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest


class TraceItemTableRequestVisitor(ABC):
    """
    When you call visitor.visit(msg), the visitor will call the appropriate visit function
    visit_TraceItemTableRequest, visit_Column, visit_AttributeAggregation, etc.
    Subclasses are responsible for implementing these functions.
    """

    def visit(self, node: Message) -> None:
        method_name = "visit_" + type(node).__name__
        visitor = getattr(self, method_name, self.generic_visit)
        visitor(node)

    def generic_visit(self, node: Message) -> None:
        raise NotImplementedError(f"No visit_{type(node).__name__} method")


class NormalizeFormulaLabels(TraceItemTableRequestVisitor):
    """
    Adds a label to any formula that doesn't have one.
    """

    def visit_TimeSeriesRequest(self, node: TraceItemTableRequest) -> None:
        pass
