from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Iterable, Optional, Sequence, Union


class Node(ABC):
    """
    Abstract representation of anything iterable on a query.
    The query designed as a tree. The root of the tree is the Query class.
    The query class has several branches, which are almost all collections.
    These branches represent selected_columns, conditions, etc.
    All the elements of all those collections are subclasses of Node.
    """
    @abstractmethod
    def format(self) -> str:
        """
        Turn this node into a string. for Clickhouse
        TODO: provide a clickhouse formatter to this method so that, through
        a strategy pattern, this class will provide the content for the query
        and the clickhouse formatter will provide the format.
        """
        raise NotImplementedError

    @abstractmethod
    def map(self, closure: Callable[[Node], Node]) -> Node:
        """
        Applies the provided function to this object (and potentially to its
        children when present) and returns the Node this one should be replaced
        with (if any replacement is needed).

        The closure takes in a Node and return a node to replace it like for a
        map function over collections.

        Some specific corner case are defined in the subclasses.

        TODO: Consider making the closure more type specific. I am not certain
        whether it will be better to have dedicated map functions (exposed
        separately on the Query object) or one single map function that iterates
        on the entire tree and let the callsite decide how to interpret a Node.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, closure: Callable[[Node], bool]) -> bool:
        """
        Applies the provided function to this object (and potentially to its
        children when present) and removes the children where the function returns
        False. If this function returns false, the parent Node will remove this Node.

        Some specific corner case are defined in the subclasses.

        TODO: Same concern about typing as above.
        """
        raise NotImplementedError

    @abstractmethod
    def iterate(self) -> Iterable[Node]:
        """
        Returns an iterable that will iterate over all the children of this Node.
        This can be used to iterate over the entire query to get the list of
        referenced columns.
        """
        raise NotImplementedError


class AliasedNode(ABC, Node):
    """
    Abstract representation of a node that can be given an alias in a query.
    """

    def __init__(self, alias: Optional[str]):
        self.__alias = alias

    def _format_impl(self) -> str:
        raise NotImplementedError


class Expression(ABC, AliasedNode):
    """
    Abstract representation of something that can be evaluated in isolation.
    """


class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    def __init__(self,
        column_name: str,
        table_name: Optional[str],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__column_name = column_name
        self.__table_name = table_name


class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse
    """

    def __init__(self,
        function_name: str,
        parameters: Sequence[Expression],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters


class Aggregation(AliasedNode):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """

    def __init__(
        self,
        function_name: str,
        parameters: Sequence[Expression],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters


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


class Condition(Node):
    pass


class AndCondition(Condition):
    """
    Represents a series of conditions joined with AND
    """

    def __init__(self, sub_conditions: Sequence[Condition]):
        self.__sub_conditions = sub_conditions


class OrCondition(Condition):
    """
    Represents a series of conditions joined with OR
    """

    def __init__(self, sub_conditions: Sequence[Condition]):
        self.__sub_conditions = sub_conditions


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
