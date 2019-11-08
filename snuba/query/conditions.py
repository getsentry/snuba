from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterator, Sequence

from snuba.query.expressions import Expression, ExpressionContainer
from snuba.query.nodes import Node


class Operator(Enum):
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    EQ = "="
    NEQ = "!="
    IN = "IN"
    NOT_IN = "NOT IN"
    IS = "IS"
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"


class BooleanOperator(Enum):
    AND = "AND"
    OR = "OR"


class Condition(Node):
    """
    Represents a condition node in the query. This can be a simple infix
    notation query or a complex query built by a nested boolean condition.
    This object can be used in WHERE and HAVING clauses, not in others.
    """
    @abstractmethod
    def get_expressions(self) -> ExpressionContainer:
        """
        Provides an expression container to navigate through the expressions
        contained (directly or transitively) in this condition and to
        manipulate them.
        """
        raise NotImplementedError


class ComplexConditionExpressionContainer(ExpressionContainer):
    """
    Abstracts the expressions transitivey contained in a Condition node and
    provide an ExpressionContainer view over them.
    """

    def __init__(self, children: Sequence[Condition]) -> None:
        self.children = children

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        for c in self.children:
            c.get_expressions().transform(func)

    def __iter__(self) -> Iterator[Expression]:
        for c in self.children:
            for e in c.get_expressions():
                yield e


@dataclass
class BooleanCondition(Condition):
    """
    A boolean condition between two other conditions.
    """
    lhs: Condition
    operator: BooleanOperator
    rhs: Condition

    def format(self) -> str:
        raise NotImplementedError

    def get_expressions(self) -> ExpressionContainer:
        return ComplexConditionExpressionContainer([self.lhs, self.rhs])


@dataclass
class NotCondition(Condition):
    """
    A not condition
    """
    condition: Condition

    def format(self) -> str:
        raise NotImplementedError

    def get_expressions(self) -> ExpressionContainer:
        return ComplexConditionExpressionContainer([self.condition])


@dataclass
class BasicCondition(Condition):
    """
    Represents a condition in the form `expression` `operator` `expression`
    """
    lhs: Expression
    operator: Operator
    rhs: Expression

    def format(self) -> str:
        raise NotImplementedError

    def get_expressions(self) -> ExpressionContainer:
        return BasicConditionExpressionContainer(self)


class BasicConditionExpressionContainer(ExpressionContainer):
    """
    Abstracts the expressions transitivey contained in a basic Condition node and
    provides an ExpressionContainer view over them.
    """

    def __init__(self, wrapped: BasicCondition) -> None:
        self.wrapped = wrapped

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        """
        Applies the transformation to the expressions referenced by lhs and rhs
        """
        self.wrapped.lhs = self._transform_children((self.wrapped.lhs,), func)[0]
        self.wrapped.rhs = self._transform_children((self.wrapped.rhs,), func)[0]

    def __iter__(self) -> Iterator[Expression]:
        """
        Iterates over the expressions referenced by lhs and rhs
        """
        for e in self._iterate_over_children([self.wrapped.lhs]):
            yield e
        for e in self._iterate_over_children([self.wrapped.rhs]):
            yield e
