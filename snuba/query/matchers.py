from __future__ import annotations

from abc import ABC, abstractmethod
from collections import ChainMap
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Generic, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr

ScalarType = Optional[Union[str, int, float, date, datetime]]
MatchType = Union[Expression, ScalarType]
TNode = TypeVar("TNode", bound=MatchType)


@dataclass(frozen=True)
class MatchResult:
    """
    Contains the parameters found in an expression by a Pattern.
    Parameters are the equivalent to groups in regular expressions, they
    are named parts of the expression we are matching that are identified
    when a valid match is found.
    """

    results: Mapping[str, MatchType] = field(default_factory=dict)

    def expression(self, name: str) -> Expression:
        ret = self.results[name]
        assert isinstance(ret, Expression)
        return ret

    def scalar(self, name: str) -> ScalarType:
        ret = self.results[name]
        assert ret is None or isinstance(ret, (str, int, float, date, datetime))
        return ret

    def merge(self, values: MatchResult) -> MatchResult:
        return MatchResult(ChainMap(self.results, values.results))


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
class Param(Pattern[TNode]):
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
        return result.merge(MatchResult({self.name: node}))


@dataclass(frozen=True)
class AnyExpression(Pattern[Expression]):
    """
    Match any expression of the type provided.
    This allows us to match any Expression since the Any class cannot
    match abstract classes (like Expression)
    """

    def match(self, node: Expression) -> Optional[MatchResult]:
        return MatchResult()


@dataclass(frozen=True)
class AnyString(Pattern[Optional[str]]):
    """
    Match any string including the None value
    """

    def match(self, node: Optional[str]) -> Optional[MatchResult]:
        return MatchResult() if node is None or isinstance(node, str) else None


@dataclass(frozen=True)
class Any(Pattern[TNode]):
    """
    Match any concrete expression/scalar of the type provided
    """

    type: Type[TNode]

    def match(self, node: TNode) -> Optional[MatchResult]:
        return MatchResult() if isinstance(node, self.type) else None


@dataclass(frozen=True)
class OptionalString(Pattern[Optional[str]]):
    """
    Matches one specific string (or None).
    """

    value: Optional[str]

    def match(self, node: Optional[str]) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class String(Pattern[str]):
    """
    Matches one specific string.
    """

    value: Optional[str]

    def match(self, node: str) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class Or(Pattern[TNode]):
    """
    Union of multiple patterns. Matches if at least one is a valid match
    and returns the first valid one.
    """

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
    provided. If one of these Patterns is left None, that field will be ignored
    when matching (equivalent to Any, but less verbose).
    """

    alias: Optional[Pattern[Optional[str]]] = None
    column_name: Optional[Pattern[str]] = None
    table_name: Optional[Pattern[Optional[str]]] = None

    def match(self, node: Expression) -> Optional[MatchResult]:
        if not isinstance(node, ColumnExpr):
            return None

        result = MatchResult()
        for pattern, value in (
            (self.alias, node.alias),
            (self.table_name, node.table_name),
        ):
            if pattern is not None:
                partial_result = pattern.match(value)
                if not partial_result:
                    return None
                result = result.merge(partial_result)

        if self.column_name is not None:
            partial_result = self.column_name.match(node.column_name)
            if not partial_result:
                return None
            result = result.merge(partial_result)

        return result


@dataclass(frozen=True)
class FunctionCall(Pattern[Expression]):
    """
    Matches a Function in the AST expression.
    It works like the Column Pattern. if alias, function_name and function_parameters
    are provided, they have to match, otherwise they are ignored.
    """

    alias: Optional[Pattern[Optional[str]]] = None
    function_name: Optional[Pattern[str]] = None
    parameters: Optional[Tuple[Pattern[Expression], ...]] = None
    # Specifies whether we allow optional parameters when matching.
    # if this is False, all patterns of the function to match must match
    # one by one. If with_optionals is True, this will allow additional
    # parameters to exist in the function to match that are not present
    # in this pattern.
    with_optionals = bool = False

    def match(self, node: Expression) -> Optional[MatchResult]:
        if not isinstance(node, FunctionCallExpr):
            return None

        result = MatchResult()
        if self.alias is not None:
            partial_result = self.alias.match(node.alias)
            if not partial_result:
                return None
            result = result.merge(partial_result)
        if self.function_name is not None:
            partial_result = self.function_name.match(node.function_name)
            if not partial_result:
                return None
            result = result.merge(partial_result)

        if self.parameters:
            if not self.with_optionals:
                if len(self.parameters) != len(node.parameters):
                    return None
            else:
                if len(self.parameters) > len(node.parameters):
                    return None

            for index, param_pattern in enumerate(self.parameters):
                p_result = param_pattern.match(node.parameters[index])
                if not p_result:
                    return None
                else:
                    result = result.merge(p_result)

        return result


# TODO: Add more Patterns when needed.
