from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any as AnyType
from typing import Generic, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.expressions import OptionalScalarType
from snuba.query.expressions import SubscriptableReference as SubscriptableReferenceExpr

MatchType = Union[Expression, OptionalScalarType]

TMatchedType = TypeVar("TMatchedType", covariant=True)


@dataclass(frozen=True)
class MatchResult:
    """
    Contains the parameters found in an expression by a Pattern.
    Parameters are the equivalent to groups in regular expressions,
    they are named parts of the expression we are matching that are
    identified when a valid match is found.
    """

    results: Mapping[str, MatchType] = field(default_factory=dict)

    def contains(self, name: str) -> bool:
        return name in self.results and self.results[name] is not None

    def expression(self, name: str) -> Expression:
        """
        Return an expression from the results given the name of the
        parameter it matched.
        Since we cannot match None Expressions (because they do not
        exist in the AST) this does not need to return Optional.
        """
        ret = self.results[name]
        assert isinstance(ret, Expression)
        return ret

    def scalar(self, name: str) -> OptionalScalarType:
        """
        Return a scalar from the results given the name of the parameter
        it matched.
        Scalars are all individual types in the AST that are not
        structured expressions.
        There are several places in the AST where a scalar can be None
        (like aliases) thus it is possible that the matched node is None,
        thus we need to be able to return None.
        """
        ret = self.results[name]
        assert ret is None or isinstance(ret, (str, int, float, date, datetime))
        return ret

    def string(self, name: str) -> str:
        """
        Returns a string present in the result, guaranteeing the string
        is there or throws.
        """
        ret = self.results[name]
        assert isinstance(ret, str), type(ret)
        return ret

    def optional_string(self, name: str) -> Optional[str]:
        """
        Returns a string present in the result or it is None.
        """
        ret = self.results[name]
        assert ret is None or isinstance(ret, str)
        return ret

    def integer(self, name: str) -> int:
        """
        Returns a int present in the result, guaranteeing the int is there
        or throws.
        """
        ret = self.results[name]
        assert isinstance(ret, int)
        return ret

    # TODO: Consider adding more utility method to assert and return specific types
    # from the Results if needed.

    def merge(self, values: MatchResult) -> MatchResult:
        return MatchResult({**self.results, **values.results})


class Pattern(ABC, Generic[TMatchedType]):
    """
    Tries to match a given node (like an AST Expression) with the rules
    provided when instantiating this class. This is meant to work like
    a regular expression but for AST expressions.

    When a node is provided to this method, if this method returns an
    instance of MatchResult, it means the node fully matches the pattern.
    If the pattern defines named parameters, their value is returned
    as part of the MatchResult object returned.
    More on how to define parameters in the Param class.

    This is not supposed to ever throw as long as the node is properly
    is a valid data structure, even if it does not match the pattern.

    This is parametric but the parameter is not used in the class.
    This is because the parameter is meant only to statically type check
    that patterns are composed in a way that makes sense (like not try
    to match a table name as a Column since it is impossible). It
    represent the type effectively matched by the expression thus covariant,
    not the type of the object the Pattern can try to match so that
    Pattern[Column] is a subtype of Pattern[Expression].
    """

    @abstractmethod
    def match(self, node: AnyType) -> Optional[MatchResult]:
        """
        Returns a MatchResult if the node provided matches this pattern
        otherwise it returns None.

        The parameter is not TMatchedType in that TMatchedType is the
        type of the Node effectively matched. That would be a subtype
        of the type we try to match.
        For example a Column Pattern should be able to get an Expression
        and match a Column. TMatchedType is Column.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Param(Pattern[TMatchedType]):
    """
    Defines a named parameter in a Pattern. When matching the overall
    Pattern, if the Pattern nested in this object matches, the
    corresponding input node will be given a name and returned as part
    of the MatchResult return value. This is meant to represent what
    a group is in a regex.

    Params can be nested, and they can wrap any valid Pattern.

    There is no defined behavior for duplicated parameter names in a Pattern.
    These names should be disjoint for a deterministic behavior.
    """

    name: str
    pattern: Pattern[TMatchedType]

    def match(self, node: AnyType) -> Optional[MatchResult]:
        result = self.pattern.match(node)
        if result is None:
            return None
        return result.merge(MatchResult({self.name: node}))


@dataclass(frozen=True)
class AnyExpression(Pattern[Expression]):
    """
    Matches any expression of the type provided.
    This allows us to match any Expression since the Any class cannot
    match abstract classes (like Expression)
    """

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if isinstance(node, Expression) else None


@dataclass(frozen=True)
class Any(Pattern[TMatchedType]):
    """
    Match any concrete expression/scalar of the type provided.
    """

    type: Type[TMatchedType]

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if isinstance(node, self.type) else None


@dataclass(frozen=True)
class String(Pattern[str]):
    """
    Matches one specific string.
    """

    value: str

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class Integer(Pattern[int]):
    """
    Matches one specific integer.
    """

    value: int

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class OptionalString(Pattern[Optional[str]]):
    """
    Matches one specific string (or None).
    """

    value: Optional[str]

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if node == self.value else None


@dataclass(frozen=True)
class AnyOptionalString(Pattern[Optional[str]]):
    """
    Matches any string including the None value. This cannot be done with
    Any(type) because that cannot match Union[str, None].
    """

    def match(self, node: AnyType) -> Optional[MatchResult]:
        return MatchResult() if node is None or isinstance(node, str) else None


@dataclass(frozen=True)
class Or(Pattern[TMatchedType]):
    """
    Union of multiple patterns. Matches if at least one is a valid match
    and returns the first valid one.
    """

    patterns: Sequence[Pattern[TMatchedType]]

    def match(self, node: AnyType) -> Optional[MatchResult]:
        for p in self.patterns:
            ret = p.match(node)
            if ret:
                return ret
        return None


@dataclass(frozen=True)
class Column(Pattern[ColumnExpr]):
    """
    Matches a Column in an AST expression.
    The column is defined by alias, name and table. For each,
    a Pattern can be provided. If one of these Patterns is
    left None, that field will be ignored when matching
    (equivalent to Any, but less verbose).
    """

    table_name: Optional[Pattern[Optional[str]]] = None
    column_name: Optional[Pattern[str]] = None

    def match(self, node: AnyType) -> Optional[MatchResult]:
        if not isinstance(node, ColumnExpr):
            return None

        result = MatchResult()
        for pattern, value in (
            (self.table_name, node.table_name),
            (self.column_name, node.column_name),
        ):
            if pattern is not None:
                partial_result = pattern.match(value)
                if partial_result is None:
                    return None
                result = result.merge(partial_result)

        return result


@dataclass(frozen=True)
class Literal(Pattern[LiteralExpr]):
    value: Optional[Pattern[OptionalScalarType]] = None

    def match(self, node: AnyType) -> Optional[MatchResult]:
        if not isinstance(node, LiteralExpr):
            return None

        if self.value is not None:
            return self.value.match(node.value)
        else:
            return MatchResult()


@dataclass(frozen=True)
class FunctionCall(Pattern[FunctionCallExpr]):
    """
    Matches a Function in the AST expression.
    It works like the Column Pattern. if alias, function_name and function_parameters
    are provided, they have to match, otherwise they are ignored.
    """

    function_name: Optional[Pattern[str]] = None
    # This is a tuple instead of a sequence to match the data structure
    # we use in the actual FunctionCall class. There it has to be a tuple
    # to be hashable.
    parameters: Optional[Tuple[Pattern[Expression], ...]] = None
    # Specifies whether we allow optional parameters when matching.
    # if this is False, all patterns of the function to match must match
    # one by one. If with_optionals is True, this will allow additional
    # parameters to exist in the function to match that are not present
    # in this pattern. When it is False the parameters of the FunctionCall
    # must match one by one the ones of the Pattern thus the two tuples
    # must have the same length.
    with_optionals: bool = False
    # Specifies a type that all the parameters in a function must be.
    # If it is set, then it will iterate through the parameters and
    # check them against the type. If this is set, it's not necessary
    # to also specify the parameters field.
    all_parameters: Optional[Pattern[Expression]] = None

    def match(self, node: AnyType) -> Optional[MatchResult]:
        if not isinstance(node, FunctionCallExpr):
            return None
        result = MatchResult()
        if self.function_name is not None:
            partial_result = self.function_name.match(node.function_name)
            if partial_result is None:
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
                if p_result is None:
                    return None
                else:
                    result = result.merge(p_result)

        if self.all_parameters:
            for p in node.parameters:
                if not self.all_parameters.match(p):
                    return None

        return result


@dataclass(frozen=True)
class SubscriptableReference(Pattern[SubscriptableReferenceExpr]):
    """
    Matches a SubscriptableReference in the AST expression.
    If column_name and key arguments are provided, they have to match, otherwise they are ignored.
    """

    table_name: Optional[Pattern[Optional[str]]] = None
    column_name: Optional[Pattern[str]] = None
    key: Optional[Pattern[str]] = None

    def match(self, node: AnyType) -> Optional[MatchResult]:
        if not isinstance(node, SubscriptableReferenceExpr):
            return None

        result = MatchResult()

        if self.table_name is not None:
            partial_result = self.table_name.match(node.column.table_name)
            if partial_result is None:
                return None
            result = result.merge(partial_result)

        if self.column_name is not None:
            partial_result = self.column_name.match(node.column.column_name)
            if partial_result is None:
                return None
            result = result.merge(partial_result)

        if self.key is not None:
            partial_result = self.key.match(node.key.value)
            if partial_result is None:
                return None
            result = result.merge(partial_result)

        return result


# TODO: Add more Patterns when needed.
