from __future__ import annotations

from abc import abstractmethod
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
    This object can be used in WHERE and HAVING clauses, not in others.
    """
    @abstractmethod
    def get_expressions(self) -> ExpressionContainer:
        """
        The Condition class provides access to expressions in the query (the
        operands of the condition).
        The condition itself cannot be an ExpressionContainer because
        composite conditions are already ConditionContainers and inheriting
        from both would cause naming collision.
        Thus We use this method to have access to an ExpressionContainer that
        represent all the expressions referenced in this condition.
        """
        raise NotImplementedError


class ConditionContainer(NodeContainer[Condition]):
    """
    This class exists because we need, in some cases, to check the type
    of a container at runtime and to ensure a Node is a NodeContainer[Condition].
    Since the parameters disappear at runtime, thus something like
    isinstance(a, NodeContainer[Condition]) would not work, we need to make
    NodeContainer[Condition] a class.
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

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        for condition in self.__condition.sub_conditions:
            condition.get_expressions().transform(func)


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

    def transform(self, func: Callable[[Condition], Condition]) -> None:
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

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        self.__condition.lhs, self.__condition.rhs = self._transform_children(
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
