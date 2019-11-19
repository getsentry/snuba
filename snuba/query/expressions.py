from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable, Iterator, Optional, Sequence, Union


@dataclass(frozen=True)
class Expression(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST), literals, etc.

    All expressions can have an optional alias.
    """

    alias: Optional[str]

    @abstractmethod
    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms this expression through the function passed in input.
        This works almost like a map function over sequences though, contrarily to
        sequences, this acts on a subtree. The semantics of transform can be different
        between intermediate nodes and leaves, so each node class can implement it
        its own way.

        All expressions are frozen dataclasses. This means they are immutable and
        format will either return self or a new instance. It cannot transform the
        expression in place.
        """
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[Expression]:
        """
        Used to iterate over this expression and its children. The exact
        semantics depends on the structure of the expression.
        See the implementations for more details.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Literal(Expression):
    """
    A literal in the SQL expression
    """
    value: Union[None, bool, str, float, int]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self


@dataclass(frozen=True)
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """
    column_name: str
    table_name: Optional[str]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self


@dataclass(frozen=True)
class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse.
    This class also represent conditions. Since Clickhouse supports both the conventional
    infix notation for condition and the functional one, we converge into one
    representation only in the AST to make query processing easier.
    A query processor would not have to care of processing both functional conditions
    and infix conditions.

    This function call abstraction can support both functions with one parameters group
    like f(X,Y) and functions with two parameters groups f(X, Y)(Z).
    Since we are not doing anything except transforming and iterating over these functions
    there is no need to have a subclass for these two cases.
    """
    function_name: str
    parameters_group1: Sequence[Expression]
    # This is for expressions like f(x)(y).
    # None means there is no second gorup of parameters
    parameters_group2: Optional[Sequence[Expression]] = None

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms the subtree starting from the children and then applying
        the transformation function to the root.
        This order is chosen to make the semantics of transform more meaningful,
        the transform operation will be performed on the children first (think
        about the parameters of a function call) and then to the node itself.

        The consequence of this is that, if the transformation function replaces
        the root with something else, with different children, we trust the
        transformation function and we do not run that same function over the
        new children.
        """
        transformed = replace(
            self,
            parameters_group1=list(map(lambda child: child.transform(func), self.parameters_group1)),
            parameters_group2=list(map(lambda child: child.transform(func), self.parameters_group2))
            if self.parameters_group2 is not None
            else None
        )
        return func(transformed)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        The order here is arbitrary, postfix is chosen to follow the same
        order we have in the transform method.
        """
        for child in self.parameters_group1:
            for sub in child:
                yield sub
        if self.parameters_group2:
            for child in self.parameters_group2:
                for sub in child:
                    yield sub
        yield self
