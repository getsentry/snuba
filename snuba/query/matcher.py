from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Generic, Mapping, Optional, Sequence, Tuple, TypeVar

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression, FunctionCall

TNode = TypeVar("TNode")


@dataclass(frozen=True)
class MatchResult:
    """
    Contains the parameters found in an expression by a Pattern.
    Parameters are the equivalent to groups in regular expressions, they
    are named parts of the expression we are matching that are identified
    when a valid match is found.
    """

    expressions: Mapping[str, Expression] = field(default_factory=dict)
    strings: Mapping[str, Optional[str]] = field(default_factory=dict)
    ints: Mapping[str, Optional[int]] = field(default_factory=dict)
    floats: Mapping[str, Optional[float]] = field(default_factory=dict)
    dates: Mapping[str, Optional[date]] = field(default_factory=dict)
    datetimes: Mapping[str, Optional[datetime]] = field(default_factory=dict)

    def merge(self, values: MatchResult) -> MatchResult:
        return MatchResult(
            expressions={**self.expressions, **values.expressions},
            strings={**self.strings, **values.strings},
            ints={**self.ints, **values.ints},
            floats={**self.floats, **values.floats},
            dates={**self.dates, **values.dates},
            datetimes={**self.datetimes, **values.datetimes},
        )


class Pattern(ABC, Generic[TNode]):
    """
    Tries to match a given node (like an AST Expression) with the rules provided
    when instantiating this class. This is meant to work like a regular expression
    but for AST expressions.

    When a node is provided to this method, if this method return an instance of
    MatchResult it means the node fully matches the pattern.
    If the pattern defines named parameters, their value is returned as part of
    the MatchResult object returned.
    More on how to define parameters in the Param class.

    This is not supposed to ever throw as long as the node is properly is a valid
    data structure, even if it does not match the pattern.
    """

    @abstractmethod
    def match(self, node: TNode) -> Optional[MatchResult]:
        """
        Returns a MatchResult if the node provided matches this pattern
        otherwise it returns None.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Param(Pattern[TNode], Generic[TNode]):
    """
    Defines a named parameter in a Pattern. When matching the overall Pattern,
    if the Pattern nested in this object matches, the corresponding input node
    will be given a name and returned as part of the MatchResult return value.
    This is meant to represent what a group is in a regex.

    Params can be nested, and they can wrap any valid Pattern.
    """

    name: str
    pattern: Pattern[TNode]

    def match(self, node: TNode) -> Optional[MatchResult]:
        result = self.pattern.match(node)
        if not result:
            return None

        return result.merge(
            MatchResult(
                expressions={self.name: node} if isinstance(node, Expression) else {},
                strings={self.name: node} if isinstance(node, str) else {},
                ints={self.name: node} if isinstance(node, int) else {},
                floats={self.name: node} if isinstance(node, float) else {},
                dates={self.name: node} if isinstance(node, date) else {},
                datetimes={self.name: node} if isinstance(node, datetime) else {},
            )
        )


@dataclass(frozen=True)
class AnyExpression(Pattern[Expression]):
    """
    Successfully matches any expression
    """

    def match(self, node: Expression) -> Optional[MatchResult]:
        return MatchResult()


@dataclass(frozen=True)
class AnyString(Pattern[Optional[str]]):
    """
    Successfully matches any string
    """

    def match(self, node: Optional[str]) -> Optional[MatchResult]:
        return MatchResult()


@dataclass(frozen=True)
class String(Pattern[Optional[str]]):
    """
    Matches one specific string (or None).
    """

    value: Optional[str]

    def match(self, node: Optional[str]) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class Or(Pattern[TNode], Generic[TNode]):
    patterns: Sequence[Pattern[TNode]]

    def match(self, node: TNode) -> Optional[MatchResult]:
        for p in self.patterns:
            ret = p.match(node)
            if ret:
                return ret
        return None


@dataclass(frozen=True)
class Column(Pattern[Expression]):
    """
    Matches a Column in an AST expression.
    The column is defined by alias, name and table. For each a Pattern can be
    provided. If one of these Patters is left None, that field will be ignored
    when matching (equivalent to Any, but less verbose).
    """

    alias: Optional[Pattern[Optional[str]]] = None
    column_name: Optional[Pattern[Optional[str]]] = None
    table_name: Optional[Pattern[Optional[str]]] = None

    def match(self, node: Expression) -> Optional[MatchResult]:
        if not isinstance(node, ColumnExpr):
            return None

        result = MatchResult()
        for pattern, value in (
            (self.alias, node.alias),
            (self.column_name, node.column_name),
            (self.table_name, node.table_name),
        ):
            if pattern:
                partial_result = pattern.match(value)
                if not partial_result:
                    return None
                result = result.merge(partial_result)

        return result


@dataclass(frozen=True)
class Function(Pattern[Expression]):
    """
    Matches a Function in the AST expression.
    It works like the Column Pattern. if alias, function_name and function_parameters
    are provided, they have to match, otherwise they are ignored.
    """

    alias: Optional[Pattern[Optional[str]]] = None
    function_name: Optional[Pattern[Optional[str]]] = None
    function_parameters: Optional[Tuple[Pattern[Expression], ...]] = None

    def match(self, node: Expression) -> Optional[MatchResult]:
        if not isinstance(node, FunctionCall):
            return None

        result = MatchResult()
        for pattern, value in (
            (self.alias, node.alias),
            (self.function_name, node.function_name),
        ):
            if pattern:
                partial_result = pattern.match(value)
                if not partial_result:
                    return None
                result = result.merge(partial_result)

        if self.function_parameters:
            if len(self.function_parameters) != len(node.parameters):
                return None
            for index, param_pattern in enumerate(self.function_parameters):
                p_result = param_pattern.match(node.parameters[index])
                if not p_result:
                    return None
                else:
                    result = result.merge(p_result)

        return result


# TODO: Add more Patterns when needed.
