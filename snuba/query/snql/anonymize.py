from typing import Optional, Sequence, Type, Union

from snuba.clickhouse.formatter.nodes import (
    FormattedNode,
    FormattedQuery,
    FormattedSubQuery,
    PaddingNode,
    StringNode,
)
from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import Expression, ExpressionVisitor
from snuba.query.logical import Query
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.anonymize_visitor import AnonymizeAndStringifySnQLVisitor


def format_snql_anonymized(
    query: Union[LogicalQuery, CompositeQuery[Entity]]
) -> FormattedQuery:

    return FormattedQuery(
        _format_query_content(query, AnonymizeAndStringifySnQLVisitor)
    )


class StringQueryFormatter(
    DataSourceVisitor[FormattedNode, Entity], JoinVisitor[FormattedNode, Entity]
):
    def __init__(self, expression_formatter_type: Type[ExpressionVisitor[str]]):
        self.__expression_formatter_type = expression_formatter_type

    def _visit_simple_source(self, data_source: Entity) -> StringNode:
        sample_val = data_source.sample
        sample_str = f" SAMPLE {sample_val}" if sample_val is not None else ""
        return StringNode(f"{data_source.human_readable_id}{sample_str}")

    def _visit_join(self, data_source: JoinClause[Entity]) -> StringNode:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> FormattedSubQuery:
        assert isinstance(data_source, LogicalQuery)
        return self._visit_query(data_source)

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> FormattedSubQuery:
        return self._visit_query(data_source)

    def _visit_query(
        self, data_source: Union[Query, CompositeQuery[Entity]]
    ) -> FormattedSubQuery:
        return FormattedSubQuery(
            _format_query_content(data_source, self.__expression_formatter_type)
        )

    def visit_individual_node(self, node: IndividualNode[Entity]) -> StringNode:
        return StringNode(f"{node.alias}, {self.visit(node.data_source)}")

    def visit_join_clause(self, node: JoinClause[Entity]) -> StringNode:
        left = f"LEFT {node.left_node.accept(self)}"
        type = f"TYPE {node.join_type}"
        right = f"RIGHT {node.right_node.accept(self)}\n"
        on = "".join(
            [
                f"{c.left.table_alias}.{c.left.column} {c.right.table_alias}.{c.right.column}"
                for c in node.keys
            ]
        )

        return StringNode(f"{left} {type} {right} ON {on}")


def _format_query_content(
    query: Union[LogicalQuery, CompositeQuery[Entity]],
    expression_formatter_type: Type[ExpressionVisitor[str]],
) -> Sequence[FormattedNode]:
    formatter = expression_formatter_type()

    return [
        v
        for v in [
            PaddingNode(
                "MATCH",
                StringQueryFormatter(expression_formatter_type).visit(
                    query.get_from_clause()
                ),
            ),
            _format_select(query, formatter),
            _format_groupby(query, formatter),
            _build_optional_string_nodes(
                "ARRAY JOIN", query.get_arrayjoin(), formatter
            ),
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
            _build_optional_string_node("HAVING", query.get_having(), formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
            _format_offset(query, formatter),
            _format_granularity(query, formatter),
            _format_totals(query, formatter),
        ]
        if v is not None
    ]


def _format_select(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> StringNode:
    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns()
    ]
    return StringNode(f"SELECT {', '.join(selected_cols)}")


def _format_groupby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_groupby = query.get_groupby()
    if ast_groupby:
        selected_cols = [e.accept(formatter) for e in ast_groupby]
        return StringNode(f"BY {', '.join(selected_cols)}")
    return None


def _format_orderby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_orderby = query.get_orderby()
    if ast_orderby:
        orderby = [
            f"{e.expression.accept(formatter)} {e.direction.value}" for e in ast_orderby
        ]
        return StringNode(f"ORDER BY {', '.join(orderby)}")
    else:
        return None


def _build_optional_string_node(
    name: str, expression: Optional[Expression], formatter: ExpressionVisitor[str],
) -> Optional[StringNode]:
    return (
        StringNode(f"{name} {expression.accept(formatter)}")
        if expression is not None
        else None
    )


def _build_optional_string_nodes(
    name: str,
    expressions: Optional[Sequence[Expression]],
    formatter: ExpressionVisitor[str],
) -> Optional[StringNode]:
    return (
        StringNode(
            f"{name} {', '.join(expression.accept(formatter) for expression in expressions)}"
        )
        if expressions is not None
        else None
    )


def _format_limitby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_limitby = query.get_limitby()

    if ast_limitby is not None:
        return StringNode(
            f"LIMIT {ast_limitby.limit} BY {', '.join(expression.accept(formatter) for expression in ast_limitby.columns)}"
        )

    return None


def _format_limit(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_limit = query.get_limit()
    return (
        StringNode(f"LIMIT {ast_limit}")
        if ast_limit is not None and ast_limit != 1000
        else None
    )


def _format_offset(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_offset = query.get_offset()
    return StringNode(f"OFFSET {ast_offset}") if ast_offset != 0 else None


def _format_granularity(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_granularity = query.get_granularity()
    return (
        StringNode(f"GRANULARITY {ast_granularity}")
        if ast_granularity is not None
        else None
    )


def _format_totals(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_totals = query.has_totals()
    return StringNode(f"TOTALS {ast_totals}") if ast_totals else None
