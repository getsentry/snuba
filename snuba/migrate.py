import logging
import re

from parsimonious.grammar import Grammar   # type: ignore
from parsimonious.nodes import Node, NodeVisitor  # type: ignore
from typing import Any, Iterable, Mapping, Sequence, Tuple

from snuba.clickhouse.columns import (
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
from snuba.datasets.schemas.tables import TableSchema

logger = logging.getLogger("snuba.migrate")


grammar = Grammar(
    r"""
    type             = basic_type / uint / float / fixedstring / enum / lowcardinality / nullable / array
    # DateTime must come before Date
    basic_type       = "DateTime" / "Date" / "IPv4" / "IPv6" / "String" / "UUID"
    uint             = "UInt" uint_size
    uint_size        = "8" / "16" / "32" / "64"
    float            = "Float" float_size
    float_size       = "32" / "64"
    fixedstring      = "FixedString" open_paren fixedstring_size close_paren
    fixedstring_size = ~r"\d+"
    enum             = "Enum" enum_size open_paren enum_pairs close_paren
    enum_size        = "8" / "16"
    enum_pairs       = (enum_pair comma* space*)*
    enum_pair        = quote enum_str quote space equal space enum_val
    enum_str         = ~r"([a-zA-Z0-9]+)"
    enum_val         = ~r"\d+"
    lowcardinality   = "LowCardinality" inner_type
    nullable         = "Nullable" inner_type
    array            = "Array" inner_type
    inner_type       = ~r"\([a-zA-Z0-9()]+\)"
    open_paren       = "("
    close_paren      = ")"
    equal            = "="
    comma            = ","
    space            = " "
    quote            = "'"
    """
)


class Visitor(NodeVisitor):
    def visit_type(self, node: Node, visited_children: Iterable[ColumnType]) -> ColumnType:
        (child,) = visited_children
        return child

    def visit_basic_type(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
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

    def visit_fixedstring(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        size = int(node.children[2].text)
        return FixedString(size)

    def visit_enum(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        _enum, _size, _open, pairs, _close = visited_children
        return Enum(pairs)

    def visit_enum_pairs(self, node: Node, visited_children: Iterable[Any]) -> Sequence[Tuple[str, int]]:
        return [c[0] for c in visited_children]

    def visit_enum_pair(self, node: Node, visited_children: Iterable[Any]) -> Tuple[str, int]:
        (_quot, enum_str, _quot, _sp, _eq, _sp, enum_val) = visited_children
        return (enum_str, enum_val)

    def visit_enum_str(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_enum_val(self, node: Node, visited_children: Iterable[Any]) -> int:
        return int(node.text)

    def visit_lowcardinality(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        return LowCardinality(self.__get_inner_type(node.children[1]))

    def visit_nullable(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        return Nullable(self.__get_inner_type(node.children[1]))

    def visit_array(self, node: Node, visited_children: Iterable[Any]) -> ColumnType:
        return Array(self.__get_inner_type(node.children[1]))

    def generic_visit(self, node: Node, visited_children: Iterable[Any]) -> Any:
        return visited_children or node

    def __get_inner_type(self, node: Node) -> ColumnType:
        inner_type = node.text[1:-1]  # Strip open/close parens
        tree = grammar.parse(inner_type)
        return Visitor().visit(tree)


def _run_schema(conn, schema):
    if not isinstance(schema, TableSchema):
        return
    clickhouse_table = schema.get_local_table_name()

    STRIP_CAST_RE = re.compile(r"^CAST\((.*), (.*)\)$", re.IGNORECASE)

    def strip_cast(default_expr: str) -> str:
        match = STRIP_CAST_RE.match(default_expr)
        if match:
            default_expr = match.groups()[0]
        return default_expr

    def get_column_type(column_type: str) -> ColumnType:
        return Visitor().visit(grammar.parse(column_type))

    def get_column(
        column_type: str, default_type: str, default_expr: str, codec_expr: str
    ) -> ColumnType:
        column = get_column_type(column_type)

        if default_type == "MATERIALIZED":
            column = Materialized(column, strip_cast(default_expr))
        elif default_type == "DEFAULT":
            column = WithDefault(column, strip_cast(default_expr))

        if codec_expr:
            column = WithCodecs(column, codec_expr.split(", "))

        return column

    def get_schema() -> Mapping[str, ColumnType]:
        return {
            column_name: get_column(column_type, default_type, default_expr, codec_expr)
            for column_name, column_type, default_type, default_expr, _comment, codec_expr in [
                cols[:6]
                for cols in conn.execute("DESCRIBE TABLE %s" % clickhouse_table)
            ]
        }

    local_schema = get_schema()

    migrations = schema.get_migration_statements()(clickhouse_table, local_schema)
    for statement in migrations:
        logger.info(f"Executing migration: {statement}")
        conn.execute(statement)

    # Refresh after alters
    refreshed_schema = get_schema()

    # Warn user about any *other* schema diffs
    differences = schema.get_column_differences(refreshed_schema)

    for difference in differences:
        logger.warn(difference)


def run(conn, dataset):
    schemas = []
    if dataset.get_table_writer():
        schemas.append(dataset.get_table_writer().get_schema())
    schemas.append(dataset.get_dataset_schemas().get_read_schema())

    for schema in schemas:
        _run_schema(conn, schema)
