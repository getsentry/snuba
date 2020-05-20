from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar


TExpIn = TypeVar("TExpIn")
TExpOut = TypeVar("TExpOut")


class ExpressionTranslator(ABC, Generic[TExpIn, TExpOut]):
    """
    Translates an expression from one AST into an expression of a different AST.
    """

    @abstractmethod
    def translate_expression(self, exp: TExpIn) -> TExpOut:
        raise NotImplementedError
