from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Sequence

from clickhouse_driver import Client
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.clickhouse.columns import (
    JSON,
    UUID,
    AggregateFunction,
    Array,
    Bool,
    ColumnType,
    Date,
    DateTime,
    DateTime64,
    Enum,
    FixedString,
    Float,
    Int,
    IPv4,
    IPv6,
    Map,
    SimpleAggregateFunction,
    String,
    UInt,
)
from snuba.clickhouse.columns import Tuple as TupleCol
from snuba.migrations.columns import MigrationModifiers

grammar = Grammar(
    r"""
    type             = primitive / lowcardinality / agg / simple_agg / nullable / array / map / tuple
    # datetime64 needs to be before basic_type to not be parsed as DateTime
    primitive        = datetime64 / basic_type / uint / int / float / fixedstring / enum / json
    # DateTime must come before Date
    basic_type       = "DateTime" / "Date" / "IPv4" / "IPv6" / "String" / "UUID" / "Bool"
    uint             = "UInt" uint_size
    int              = "Int" uint_size
    uint_size        = "8" / "16" / "32" / "64" / "128"
    float            = "Float" float_size
    float_size       = "32" / "64"
    fixedstring      = "FixedString" open_paren space* fixedstring_size space* close_paren
    fixedstring_size = ~r"\d+"
    enum             = "Enum" enum_size open_paren space* enum_pairs space* close_paren
    enum_size        = "8" / "16"
    enum_pairs       = (enum_pair (space* comma space*)?)*
    enum_pair        = quote enum_str quote space* equal space* enum_val
    enum_str         = ~r"([a-zA-Z0-9\-]+)"
    enum_val         = ~r"\d+"
    agg              = "AggregateFunction" open_paren space* agg_func space* comma space* agg_types space* close_paren
    simple_agg       = "SimpleAggregateFunction" open_paren space* agg_func space* comma space* agg_types space* close_paren
    agg_func         = ~r"[a-zA-Z0-9]+\([a-zA-Z0-9\,\.\s]+\)|[a-zA-Z0-9]+"
    agg_types        = (type (space* comma space*)?)*
    array            = "Array" open_paren space* (array / primitive / lowcardinality / nullable) space* close_paren
    map              = "Map" open_paren space* (map / primitive / lowcardinality) space* comma space* (map / primitive / lowcardinality / nullable) space* close_paren
    lowcardinality   = "LowCardinality" open_paren space* (primitive / nullable) space* close_paren
    nullable         = "Nullable" open_paren space* (primitive / basic_type) space* close_paren
    open_paren       = "("
    close_paren      = ")"
    equal            = "="
    comma            = ","
    space            = " "
    quote            = "'"
    datetime64              = "DateTime64" (open_paren datetime64_precision (comma space* quote datetime64_timezone quote)? close_paren)?
    datetime64_precision    = "3" / "6" / "9"
    datetime64_timezone     = ~r"[a-zA-Z0-9_/]+"
    tuple            = "Tuple" open_paren space* (primitive / lowcardinality / nullable) (comma space* (primitive / lowcardinality / nullable))* space* close_paren
    json             = "JSON" (open_paren space* json_params space* close_paren)?
    json_params      = json_param (comma space* json_param)*
    json_param       = json_max_dynamic_paths / json_max_dynamic_types / json_type_hint / json_skip_path / json_skip_regexp
    json_max_dynamic_paths = "max_dynamic_paths" space* equal space* ~r"\d+"
    json_max_dynamic_types = "max_dynamic_types" space* equal space* ~r"\d+"
    json_type_hint   = quote json_path quote space* type
    json_skip_path   = "SKIP" space* quote json_path quote
    json_skip_regexp = "SKIP" space* "REGEXP" space* quote json_regexp quote
    json_path        = ~r"[a-zA-Z0-9_.\[\]]+"
    json_regexp      = ~r"[^']+"
    """
)


def merge_modifiers(
    col_type: ColumnType[MigrationModifiers], modifiers: MigrationModifiers
) -> ColumnType[MigrationModifiers]:
    existing_modifiers = col_type.get_modifiers()
    if existing_modifiers is None:
        return col_type.set_modifiers(modifiers)
    else:
        return col_type.set_modifiers(existing_modifiers.merge(modifiers))


_TYPES: dict[str, type[ColumnType[MigrationModifiers]]] = {
    "Bool": Bool,
    "Date": Date,
    "DateTime": DateTime,
    "IPv4": IPv4,
    "IPv6": IPv6,
    "JSON": JSON,
    "String": String,
    "UUID": UUID,
}


class Visitor(NodeVisitor):  # type: ignore
    def visit_basic_type(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        return _TYPES[node.text]()

    def visit_uint(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        size = int(node.children[1].text)
        return UInt(size)

    def visit_int(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        size = int(node.children[1].text)
        return Int(size)

    def visit_float(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        size = int(node.children[1].text)
        return Float(size)

    def visit_fixedstring(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        size = int(node.children[3].text)
        return FixedString(size)

    def visit_enum(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        _enum, _size, _open, _sp, pairs, _sp, _close = visited_children
        return Enum(pairs)

    def visit_enum_pairs(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Sequence[tuple[str, int]]:
        return [c[0] for c in visited_children]

    def visit_enum_pair(self, node: Node, visited_children: Iterable[Any]) -> tuple[str, int]:
        (_quot, enum_str, _quot, _sp, _eq, _sp, enum_val) = visited_children
        return (enum_str, enum_val)

    def visit_enum_str(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_enum_val(self, node: Node, visited_children: Iterable[Any]) -> int:
        return int(node.text)

    def visit_agg(
        self, node: Node, visited_children: Iterable[Any]
    ) -> AggregateFunction[MigrationModifiers]:
        (
            _agg,
            _paren,
            _sp,
            agg_func,
            _sp,
            _comma,
            _sp,
            agg_types,
            _sp,
            _paren,
        ) = visited_children
        return AggregateFunction(agg_func, [*agg_types])

    def visit_simple_agg(
        self, node: Node, visited_children: Iterable[Any]
    ) -> SimpleAggregateFunction[MigrationModifiers]:
        (
            _agg,
            _paren,
            _sp,
            agg_func,
            _sp,
            _comma,
            _sp,
            agg_types,
            _sp,
            _paren,
        ) = visited_children
        return SimpleAggregateFunction(agg_func, [*agg_types])

    def visit_agg_func(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_agg_types(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Sequence[ColumnType[MigrationModifiers]]:
        return [c[0] for c in visited_children]

    def visit_lowcardinality(
        self,
        node: Node,
        visited_children: tuple[Any, Any, Any, ColumnType[MigrationModifiers], Any, Any],
    ) -> ColumnType[MigrationModifiers]:
        (_lc, _paren, _sp, inner_type, _sp, _paren) = visited_children
        return merge_modifiers(inner_type, MigrationModifiers(low_cardinality=True))

    def visit_nullable(
        self,
        node: Node,
        visited_children: tuple[Any, Any, Any, ColumnType[MigrationModifiers], Any, Any],
    ) -> ColumnType[MigrationModifiers]:
        (_null, _paren, _sp, inner_type, _sp, _paren) = visited_children
        # TODO: Remove these assertions when ColumnType will be generic
        return merge_modifiers(inner_type, MigrationModifiers(nullable=True))

    def visit_array(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        (_arr, _paren, _sp, inner_type, _sp, _paren) = visited_children
        return Array(inner_type)

    def visit_map(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        (
            _map,
            _paren,
            _sp,
            key_type,
            _sp,
            _comma,
            _sp,
            value_type,
            _sp,
            _paren,
        ) = visited_children
        return Map(key_type, value_type)

    def visit_datetime64(
        self, node: None, visited_children: Iterable[Any]
    ) -> ColumnType[MigrationModifiers]:
        (
            _type,
            precision_timezone_group,
        ) = visited_children
        if isinstance(precision_timezone_group, list) is False:
            return DateTime64()
        (
            _parenthesis,
            precision,
            timezone_group,
            _parenthesis,
        ) = precision_timezone_group
        if isinstance(timezone_group, list):
            (_comma, _space, _quote, timezone, _quote) = timezone_group
            timezone = timezone.text
        else:
            timezone = None
        return DateTime64(precision=int(precision.text), timezone=timezone)

    def visit_json(self, node: Node, visited_children: Iterable[Any]) -> JSON[MigrationModifiers]:
        (_json, params_group) = visited_children

        # Initialize default values
        max_dynamic_paths = None
        max_dynamic_types = None
        type_hints = {}
        skip_paths = []
        skip_regexp = []

        # If there are parameters, parse them
        if isinstance(params_group, list) and len(params_group) > 0:
            (_paren, _sp, params, _sp, _paren) = params_group
            # params is a list of tuples (param_type, param_value)
            for param in params:
                if isinstance(param, tuple) and len(param) == 2:
                    param_type, param_value = param
                    if param_type == "max_dynamic_paths":
                        max_dynamic_paths = param_value
                    elif param_type == "max_dynamic_types":
                        max_dynamic_types = param_value
                    elif param_type == "type_hint":
                        path, column_type = param_value
                        type_hints[path] = column_type
                    elif param_type == "skip_path":
                        skip_paths.append(param_value)
                    elif param_type == "skip_regexp":
                        skip_regexp.append(param_value)

        return JSON(
            max_dynamic_paths=max_dynamic_paths,
            max_dynamic_types=max_dynamic_types,
            type_hints=type_hints,
            skip_paths=skip_paths,
            skip_regexp=skip_regexp,
        )

    def visit_json_params(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Sequence[tuple[str, Any]]:
        """
        Handle the grammar: json_param (comma space* json_param)*

        This produces: [first_param_tuple, remaining_params_structure]
        where remaining_params_structure varies by number of parameters:
        - 2 params: [comma_node, space_node, second_param_tuple]
        - 3+ params: [[comma, space, param2], [comma, space, param3], ...]
        """
        visited_list = list(visited_children)
        result = []

        # First parameter is always at index 0
        if len(visited_list) > 0:
            result.append(visited_list[0])

        # Handle remaining parameters
        if len(visited_list) > 1 and isinstance(visited_list[1], list):
            remaining = visited_list[1]

            if len(remaining) > 0 and isinstance(remaining[0], list):
                # 3+ parameters: list of [comma, space, param] groups
                for group in remaining:
                    if isinstance(group, list) and len(group) >= 3:
                        (_comma, _space, param) = group
                        result.append(param)
            else:
                # 2 parameters: flat [comma, space, param] structure
                if len(remaining) >= 3:
                    (_comma, _space, param) = remaining
                    result.append(param)

        return result

    def visit_json_max_dynamic_paths(
        self, node: Node, visited_children: Iterable[Any]
    ) -> tuple[str, int]:
        (_name, _sp, _eq, _sp, value) = visited_children
        return ("max_dynamic_paths", int(value.text))

    def visit_json_max_dynamic_types(
        self, node: Node, visited_children: Iterable[Any]
    ) -> tuple[str, int]:
        (_name, _sp, _eq, _sp, value) = visited_children
        return ("max_dynamic_types", int(value.text))

    def visit_json_type_hint(
        self, node: Node, visited_children: Iterable[Any]
    ) -> tuple[str, tuple[str, ColumnType[MigrationModifiers]]]:
        (_quote, path, _quote, _sp, column_type) = visited_children
        return ("type_hint", (path.text, column_type))

    def visit_json_skip_path(self, node: Node, visited_children: Iterable[Any]) -> tuple[str, str]:
        (_skip, _sp, _quote, path, _quote) = visited_children
        return ("skip_path", path.text)

    def visit_json_skip_regexp(
        self, node: Node, visited_children: Iterable[Any]
    ) -> tuple[str, str]:
        (_skip, _sp, _regexp, _sp, _quote, pattern, _quote) = visited_children
        return ("skip_regexp", pattern.text)

    def visit_tuple(
        self, node: Node, visited_children: Iterable[Any]
    ) -> TupleCol[MigrationModifiers]:
        (
            _tup,
            _paren,
            _sp,
            _type,
            _additional_types,
            _sp,
            _paren,
        ) = visited_children
        types = [_type]
        if isinstance(_additional_types, list):
            for typ in _additional_types:
                if isinstance(typ, list):
                    types.append(typ[2])
                elif isinstance(typ, ColumnType):
                    types.append(typ)
                else:
                    continue
        return TupleCol(types=tuple(types))

    def generic_visit(self, node: Node, visited_children: Iterable[Any]) -> Any:
        if isinstance(visited_children, list) and len(visited_children) == 1:
            return visited_children[0]
        return visited_children or node


STRIP_CAST_RE = re.compile(r"^CAST\((.*), (.*)\)$", re.IGNORECASE)


def _strip_cast(default_expr: str) -> str:
    match = STRIP_CAST_RE.match(default_expr)
    if match:
        default_expr = match.groups()[0]
    return default_expr


def _get_column(
    column_type: str, default_type: str, default_expr: str, codec_expr: str
) -> ColumnType[MigrationModifiers]:
    column: ColumnType[MigrationModifiers] = Visitor().visit(grammar.parse(column_type))

    if default_type == "MATERIALIZED":
        column = merge_modifiers(column, MigrationModifiers(materialized=_strip_cast(default_expr)))
    elif default_type == "DEFAULT":
        column = merge_modifiers(column, MigrationModifiers(default=_strip_cast(default_expr)))

    if codec_expr:
        column = merge_modifiers(column, MigrationModifiers(codecs=codec_expr.split(", ")))

    return column


def get_local_schema(conn: Client, table_name: str) -> Mapping[str, ColumnType[MigrationModifiers]]:
    return {
        column_name: _get_column(column_type, default_type, default_expr, codec_expr)
        for column_name, column_type, default_type, default_expr, _comment, codec_expr in [
            cols[:6] for cols in conn.execute("DESCRIBE TABLE %s" % table_name).results
        ]
    }
