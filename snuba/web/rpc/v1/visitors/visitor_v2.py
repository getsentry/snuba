from abc import ABC
from typing import Any

from google.protobuf.message import Message


class RequestVisitor(ABC):
    """
    When you call visitor.visit(msg), the visitor will call the appropriate visit function
    visit_TraceItemTableRequest, visit_Column, visit_AttributeAggregation, etc.
    Subclasses are responsible for implementing these functions.
    """

    def visit(self, node: Message, *args: Any) -> None:
        method_name = "visit_" + type(node).__name__
        visitor = getattr(self, method_name, self.generic_visit)
        visitor(node, *args)

    def generic_visit(self, node: Message, *args: Any) -> None:
        raise NotImplementedError(f"No visit_{type(node).__name__} method")
