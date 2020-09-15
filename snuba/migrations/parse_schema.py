import logging
import re

from clickhouse_driver import Client
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor
from typing import Any, Iterable, Mapping, Sequence, Tuple

from snuba.clickhouse.columns import (
    AggregateFunction,
    Array,
    ColumnType,
    Date,
    DateTime,
    Enum,
    FixedString,
    Float,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nullable,
    String,
    UInt,
    UUID,
    WithCodecs,
    WithDefault,
)

logger = logging.getLogger("snuba.migrate")


grammar = Grammar(
    r"""
    type             = primitive / lowcardinality / agg / nullable / array
    primitive        = basic_type / uint / float / fixedstring / enum
    # DateTime must come before Date
    basic_type       = "DateTime" / "Date" / "IPv4" / "IPv6" / "String" / "UUID"
    uint             = "UInt" uint_size
    uint_size        = "8" / "16" / "32" / "64"
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
    agg_func         = ~r"[a-zA-Z]+\([a-zA-Z0-9\,\.\s]+\)|[a-zA-Z]+"
    agg_types        = (primitive (space* comma space*)?)*
    array            = "Array" open_paren space* (array / primitive / lowcardinality / nullable) space* close_paren
    lowcardinality   = "LowCardinality" open_paren space* (primitive / nullable) space* close_paren
    nullable         = "Nullable" open_paren space* (primitive / basic_type) space* close_paren
    open_paren       = "("
    close_paren      = ")"
    equal            = "="
    comma            = ","
    space            = " "
    quote            = "'"
    """
)


class Visitor(NodeVisitor):
    def visit_basic_type(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType:
        return {
            "Date": Date,
            "DateTime": DateTime,
            "IPv4": IPv4,
            "IPv6": IPv6,
            "String": String,
            "UUID": UUID,
        }[node.text]()

    def visit_uint(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        size = int(node.children[1].text)
        return UInt(size)

    def visit_float(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        size = int(node.children[1].text)
        return Float(size)

    def visit_fixedstring(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType:
        size = int(node.children[3].text)
        return FixedString(size)

    def visit_enum(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        _enum, _size, _open, _sp, pairs, _sp, _close = visited_children
        return Enum(pairs)

    def visit_enum_pairs(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Sequence[Tuple[str, int]]:
        return [c[0] for c in visited_children]

    def visit_enum_pair(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Tuple[str, int]:
        (_quot, enum_str, _quot, _sp, _eq, _sp, enum_val) = visited_children
        return (enum_str, enum_val)

    def visit_enum_str(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_enum_val(self, node: Node, visited_children: Iterable[Any]) -> int:
        return int(node.text)

    def visit_agg(
        self, node: Node, visited_children: Iterable[Any]
    ) -> AggregateFunction:
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
        return AggregateFunction(agg_func, *agg_types)

    def visit_agg_func(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_agg_types(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Sequence[ColumnType]:
        return [c[0] for c in visited_children]

    def visit_lowcardinality(
        self, node: Node, visited_children: Iterable[Any]
    ) -> ColumnType:
        (_lc, _paren, _sp, inner_type, _sp, _paren) = visited_children
        return LowCardinality(inner_type)

    def visit_nullable(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        (_null, _paren, _sp, inner_type, _sp, _paren) = visited_children
        return Nullable(inner_type)

    def visit_array(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        (_arr, _paren, _sp, inner_type, _sp, _paren) = visited_children
        return Array(inner_type)

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
) -> ColumnType:
    column: ColumnType = Visitor().visit(grammar.parse(column_type))

    if default_type == "MATERIALIZED":
        column = Materialized(column, _strip_cast(default_expr))
    elif default_type == "DEFAULT":
        column = WithDefault(column, _strip_cast(default_expr))

    if codec_expr:
        column = WithCodecs(column, codec_expr.split(", "))

    return column


def get_local_schema(conn: Client, table_name: str) -> Mapping[str, ColumnType]:
    return {
        column_name: _get_column(column_type, default_type, default_expr, codec_expr)
        for column_name, column_type, default_type, default_expr, _comment, codec_expr in [
            cols[:6] for cols in conn.execute("DESCRIBE TABLE %s" % table_name)
        ]
    }
