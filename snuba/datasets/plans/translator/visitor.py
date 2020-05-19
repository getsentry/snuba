from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar


# The type of an expression translated by an ExpressionMapper
TExpIn = TypeVar("TExpIn")
# The type of a generic translated expression without constraint on the data type
TExpOut = TypeVar("TExpOut")
# THe translator of an expression
TTranslator = TypeVar("TTranslator")


class ExpressionMapper(
    ABC, Generic[TExpIn, TExpOut, TTranslator],
):
    """
    One translation rule used by the MappingExpressionTranslator to translate an
    expression.

    see MappingExpressionTranslator for more context
    """

    @abstractmethod
    def attempt_map(
        self, expression: TExpIn, children_translator: TTranslator,
    ) -> Optional[TExpOut]:
        """
        Translates an expression if this rule matches such expression. If not, it
        returns None.

        It receives an instance of a MappingExpressionTranslator to take care of
        children that this rule is not capable, or not supposed to, translate.
        These node should be delegated to the children_translator.
        """
        raise NotImplementedError


class ExpressionTranslator(Generic[TExpIn, TExpOut]):
    """
    Translates a Snuba query expression into an physical query expression (like a
    Clickhouse query expression).

    The translation of every node in the expression is performed by a series of rules
    that extend ExpressionMapper.
    Rules are applied in sequence. Given an expression, the first valid rule for such
    expression is applied and the result is returned. If no rule can translate such
    expression an exception is raised.
    A rule can delegate the translation of its children back to this translator.

    Each rule only has context around the expression provided and its children. It does
    not have general context around the query or around the expression's ancestors in
    the AST.
    This approach implies that, while rules are specific to the relationship between
    dataset (later entities) and storage, this class keeps the responsibility of
    orchestrating the translation process.

    It is possible to compose different, independently defined, sets of rules that are
    applied in a single pass over the AST.
    This allows us to support joins and multi-step translations (for multi table
    storages) as an example:
    Joins can be supported by simply concatenating rule sets associated with each storage.
    Multi-step (still TODO) translations can be supported by applying a second sequence of
    rules to the result of the first one for each node in the expression to be translated.
    """

    @abstractmethod
    def translate_expression(self, exp: TExpIn) -> TExpOut:
        raise NotImplementedError
