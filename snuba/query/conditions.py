from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterator, Sequence

from snuba.query.collections import NodeContainer
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


class Condition(Node):
    """
    Represents a condition node in the query. This can be a simple infix
    notation query or a complex query built by a nested boolean condition.

    """
    @abstractmethod
    def get_expressions(self) -> ExpressionContainer:
        """
        The Condition class provides access to expressions in the query.
        Since the condition itself cannot be an ExpressionContainer because
        composite conditions are already ConditionContainers, in order to
        access the expressions in a condition, the get_expressions method
        returns ann ExpressionContainer which is an abstract container
        over a subset of the tree represented by the Condition.
        """
        raise NotImplementedError


class ConditionContainer(NodeContainer[Condition]):
    """
    This class exists to be able to preserve the NodeContainer[Condition]
    type at runtime.
    """
    pass


class CompositeConditionWrapper(ExpressionContainer):
    """
    Wraps a CompositeCondition with the goal of exposing an ExpressionContainer
    interace.
    This is meant to iterate/map over the expression directly or transitively
    contained in the wrapped Condition.
    """

    def __init__(self, condition: CompositeCondition) -> None:
        self.__condition = condition

    def __iter__(self) -> Iterator[Expression]:
        for condition in self.__condition.sub_conditions:
            for c in condition.get_expressions():
                yield c

    def map(self, func: Callable[[Expression], Expression]) -> None:
        for condition in self.__condition.sub_conditions:
            condition.get_expressions().map(func)


@dataclass
class CompositeCondition(Condition, ConditionContainer):
    """
    Represents a sequence of conditions joined with a boolean operator.
    """
    sub_conditions: Sequence[Condition]

    def get_expressions(self) -> ExpressionContainer:
        return CompositeConditionWrapper(self)

    def __iter__(self) -> Iterator[Condition]:
        for c in self.sub_conditions:
            if isinstance(c, ConditionContainer):
                for sub in c:
                    yield sub
        else:
            yield c

    def map(self, func: Callable[[Condition], Condition]) -> None:
        """
        This method is not terribly useful, will revisit whether we
        actually need it.
        """
        raise NotImplementedError


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


class BasicConditionWrapper(ExpressionContainer):
    """
    Wraps a BasicCondition exposing the ExpressionContainer interface.
    This is meant to iterate/map over the expression referenced by the
    basic condition.
    """

    def __init__(self, condition: BasicCondition) -> None:
        self.__condition = condition

    def __iter__(self) -> Iterator[Expression]:
        expressions = [self.__condition.lhs, self.__condition.rhs]
        for e in self._iterate_over_children(expressions):
            yield e

    def map(self, func: Callable[[Expression], Expression]) -> None:
        self.__condition.lhs, self.__condition.rhs = self._map_children(
            [
                self.__condition.lhs,
                self.__condition.rhs
            ],
            func,
        )


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
        return BasicConditionWrapper(self)
