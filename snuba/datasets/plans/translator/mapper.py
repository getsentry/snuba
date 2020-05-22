from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence, TypeVar

TExpIn = TypeVar("TExpIn")
TExpOut = TypeVar("TExpOut")
# The translator to be recursively applied to the children of an expression
TTranslator = TypeVar("TTranslator")


class ExpressionMapper(
    ABC, Generic[TExpIn, TExpOut, TTranslator],
):
    """
    One translation rule used by the a mapping expression translator to translate an
    expression.

    An ExpressionMapper is supposed to know how to map an expression but it is not
    required to map its children. For that it receives a translator of TTranslator
    type.

    An ExpressionMapper should not throw under any circumstance. If it does not apply to
    the input expression, it should return None.
    """

    @abstractmethod
    def attempt_map(
        self, expression: TExpIn, children_translator: TTranslator,
    ) -> Optional[TExpOut]:
        """
        Maps an expression if this rule matches such expression. If not, it returns None.
        """
        raise NotImplementedError


def apply_mappers(
    expression: TExpIn,
    mappers: Sequence[ExpressionMapper[TExpIn, TExpOut, TTranslator]],
    children_translator: TTranslator,
) -> TExpOut:
    """
    Applies several mappers in sequence to an expression and returns the result of the
    first mapper that matches.
    If no mapper capable of translating the expression is found, this throws.
    """

    for r in mappers:
        ret = r.attempt_map(expression, children_translator)
        if ret is not None:
            return ret
    raise ValueError(f"Cannot map expression {expression}")
