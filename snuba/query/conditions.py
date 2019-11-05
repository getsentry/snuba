from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Iterator, Sequence, Union

from snuba.query.collections import NodeContainer
from snuba.query.nodes import FormattableNode
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


class Condition(FormattableNode, ABC):
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
        for condition in self.__condition.get_conditions():
            for c in condition.get_expressions():
                yield c

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        for condition in self.__condition.get_conditions():
            condition.get_expressions().map(closure)


class CompositeCondition(Condition, ConditionContainer, ABC):
    """
    Represents a sequence of conditions joined with a boolean operator.
    """

    def __init__(self, sub_conditions: Sequence[Condition]):
        self.__sub_conditions = sub_conditions

    def get_conditions(self) -> Sequence[Condition]:
        return self.__sub_conditions

    def get_expressions(self) -> ExpressionContainer:
        return CompositeConditionWrapper(self)

    def __iter__(self) -> Iterator[Condition]:
        for c in self.__sub_conditions:
            if isinstance(c, ConditionContainer):
                for sub in c:
                    yield sub
        else:
            yield c

    def map(self, closure: Callable[[Condition], Condition]) -> None:
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
        expressions = [self.__condition.get_lhs(), self.__condition.get_rhs()]
        for e in self._iterate_over_children(expressions):
            yield e

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        lhs, rhs = self._map_children(
            [
                self.__condition.get_lhs(),
                self.__condition.get_rhs()
            ],
            closure,
        )
        self.__condition.set_lhs(lhs)
        self.__condition.set_rhs(rhs)


class BasicCondition(Condition):
    """
    Represents a condition in the form `expression` `operator` `expression`
    """

    def __init__(
        self,
        lhs: Expression,
        operator: Operator,
        rhs: Expression,
    ) -> None:
        self.__lhs = lhs
        self.__operator = operator
        self.__rhs = rhs

    def get_lhs(self) -> Expression:
        return self.__lhs

    def set_lhs(self, lhs: Expression) -> None:
        self.__lhs = lhs

    def get_rhs(self) -> Expression:
        return self.__rhs

    def set_rhs(self, rhs: Expression) -> None:
        self.__rhs = rhs

    def format(self) -> str:
        raise NotImplementedError

    def get_expressions(self) -> ExpressionContainer:
        return BasicConditionWrapper(self)
