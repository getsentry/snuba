from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping, Optional, Tuple, Union

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression, FunctionCall

Node = Union[Expression, Optional[str]]


class Matcher(ABC):
    @abstractmethod
    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        raise NotImplementedError


@dataclass(frozen=True)
class Param(Matcher):
    name: str
    matcher: Matcher

    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        positive_match, parameters = self.matcher.match(node)
        if not positive_match:
            return (False, {})

        return (True, {**parameters, **{self.name: node}})


@dataclass(frozen=True)
class Any(Matcher):
    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        return (True, {})


@dataclass(frozen=True)
class String(Matcher):
    value: str

    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        return (
            isinstance(node, str) and node == self.value,
            {},
        )


@dataclass(frozen=True)
class Column(Matcher):
    alias: Optional[Matcher] = None
    column_name: Optional[Matcher] = None
    table_name: Optional[Matcher] = None

    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        if not isinstance(node, ColumnExpr):
            return (False, {})

        params: Mapping[str, Node] = {}
        if self.alias:
            alias_match, alias_params = self.alias.match(node.alias)
            if not alias_match:
                return (False, {})
            else:
                params = {**params, **alias_params}

        if self.column_name:
            column_match, column_params = self.column_name.match(node.column_name)
            if not column_match:
                return (False, {})
            else:
                params = {**params, **column_params}

        if self.table_name:
            table_match, table_params = self.table_name.match(node.table_name)
            if not table_match:
                return (False, {})
            else:
                params = {**params, **table_params}

        return (True, params)


@dataclass(frozen=True)
class Function(Matcher):
    alias: Optional[Matcher] = None
    function_name: Optional[Matcher] = None
    function_parameters: Optional[Tuple[Matcher, ...]] = None

    def match(self, node: Node) -> Tuple[bool, Mapping[str, Node]]:
        if not isinstance(node, FunctionCall):
            return (False, {})

        params: Mapping[str, Node] = {}
        if self.alias:
            alias_match, alias_params = self.alias.match(node.alias)
            if not alias_match:
                return (False, {})
            else:
                params = {**params, **alias_params}

        if self.function_name:
            function_match, function_params = self.function_name.match(
                node.function_name
            )
            if not function_match:
                return (False, {})
            else:
                params = {**params, **function_params}

        if self.function_parameters:
            for p in enumerate(self.function_parameters):
                parameter_match, parameter_params = p[1].match(node.parameters[p[0]])
                if not parameter_match:
                    return (False, {})
                else:
                    params = {**params, **parameter_params}

        return (True, params)
