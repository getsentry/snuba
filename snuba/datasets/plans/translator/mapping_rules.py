from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from snuba.query.expressions import Expression, ExpressionVisitor

TExpIn = TypeVar("TExpIn", bound=Expression)
TExpOut = TypeVar("TExpOut")


class SimpleExpressionMapper(ABC, Generic[TExpIn, TExpOut]):
    @abstractmethod
    def attemptMap(self, expression: TExpIn) -> Optional[TExpOut]:
        raise NotImplementedError


class StructuredExpressionMapper(ABC, Generic[TExpIn, TExpOut]):
    @abstractmethod
    def attemptMap(
        self, expression: TExpIn, children_translator: ExpressionVisitor[TExpOut]
    ) -> Optional[TExpOut]:
        raise NotImplementedError
