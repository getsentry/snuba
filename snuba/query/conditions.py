from __future__ import annotations

from abc import ABC
from enum import Enum
from typing import Callable, Iterable, Sequence, Union

from snuba.query.nodes import FormattableNode, Node
from snuba.query.expressions import Expression


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


class Null:
    pass


class Condition(FormattableNode):
    """
    Represents a condition node in the query. This can be a simple infix
    notation query or a complex query built by a nested boolean condition.

    TODO: Support a filter method
    """
    pass


class CompositeCondition(Condition, ABC):
    """
    Represents a sequence of conditions joined with a boolean operator.
    """

    def __init__(self, sub_conditions: Sequence[Condition]):
        self.__sub_conditions = sub_conditions

    def map(self, closure: Callable[[Condition], Condition]) -> Condition:
        raise NotImplementedError

    def iterate(self) -> Iterable[Node]:
        yield self
        for c in self.__sub_conditions:
            for e in c.iterate():
                yield e


class AndCondition(CompositeCondition):
    """
    Represents a series of conditions joined with AND
    """

    def format(self) -> str:
        raise NotImplementedError


class OrCondition(CompositeCondition):
    """
    Represents a series of conditions joined with OR
    """

    def format(self) -> str:
        raise NotImplementedError


class BasicCondition(Condition):
    """
    Represents a condition in the form `expression` `operator` `expression`
    """

    def __init__(
        self,
        lhs: Expression,
        operator: Operator,
        rhs: Union[Expression, Null],
    ) -> None:
        self.__lhs = lhs
        self.__operator = operator
        self.__rhs = rhs

    def format(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Condition], Condition]) -> Condition:
        raise NotImplementedError

    def iterate(self) -> Iterable[Node]:
        yield self
        for l in self.__lhs.iterate():
            yield l
        for r in self.__rhs.iterate():
            yield r
