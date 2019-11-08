from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterator

from snuba.query.expressions import Expression, ExpressionContainer


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


class Condition(Expression, ExpressionContainer):
    """
    Represents a condition node in the query. This can be a simple infix
    notation query or a complex query built by a nested boolean condition.
    This object can be used in WHERE and HAVING clauses.
    """
    pass


@dataclass
class BinaryCondition(Condition):
    lhs: Expression
    rhs: Expression

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        self.lhs = self._transform_children((self.lhs,), func)[0]
        self.rhs = self._transform_children((self.rhs,), func)[0]

    def __iter__(self) -> Iterator[Expression]:
        yield self
        for e in self._iterate_over_children([self.lhs, self.rhs]):
            yield e


@dataclass
class BooleanCondition(BinaryCondition):
    """
    A boolean condition between two other conditions.
    """
    operator: BooleanOperator

    def format(self) -> str:
        raise NotImplementedError


@dataclass
class NotCondition(Condition):
    """
    A not condition
    """
    condition: Expression

    def format(self) -> str:
        raise NotImplementedError

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        self.condition = self._transform_children((self.condition,), func)[0]

    def __iter__(self) -> Iterator[Expression]:
        yield self
        for e in self._iterate_over_children([self.condition]):
            yield e


@dataclass
class BasicCondition(BinaryCondition):
    """
    Represents a condition in the form `expression` `operator` `expression`
    """
    operator: Operator

    def format(self) -> str:
        raise NotImplementedError
