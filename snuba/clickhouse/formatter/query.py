from __future__ import annotations

from dataclasses import replace
from typing import Optional, Sequence, Union

from snuba import settings as snuba_settings
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.formatter.nodes import (
    FormattedNode,
    FormattedQuery,
    FormattedSubQuery,
    PaddingNode,
    SequenceNode,
    StringNode,
)
from snuba.clickhouse.query import Query
from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import Expression
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings

FormattableQuery = Union[Query, CompositeQuery[Table]]


def format_query(query: FormattableQuery, settings: RequestSettings) -> FormattedQuery:
    """
    Formats a Clickhouse Query from the AST representation into an
    intermediate structure that can either be serialized into a string
    (for clickhouse) or extracted as a sequence (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.

    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """

    if isinstance(query, Query):
        if settings.get_turbo() and not query.get_from_clause().sampling_rate:
            query.set_from_clause(
                replace(
                    query.get_from_clause(),
                    sampling_rate=snuba_settings.TURBO_SAMPLE_RATE,
                )
            )
    return FormattedQuery(_format_query_content(query))


class DataSourceFormatter(DataSourceVisitor[FormattedNode, Table]):
    """
    Builds a FormattedNode out of any type of DataSource object. This
    is called when formatting the FROM clause of every nested query
    in a Composite query.
    """

    def _visit_simple_source(self, data_source: Table) -> StringNode:
        """
        Formats a simple table data source.
        """

        final = " FINAL" if data_source.final else ""
        sample = (
            f" SAMPLE {data_source.sampling_rate}" if data_source.sampling_rate else ""
        )
        return StringNode(f"{data_source.table_name}{final}{sample}")

    def _visit_join(self, data_source: JoinClause[Table]) -> FormattedNode:
        return data_source.accept(JoinFormatter())

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Table]
    ) -> FormattedSubQuery:
        assert isinstance(data_source, Query)
        return FormattedSubQuery(_format_query_content(data_source))

    def _visit_composite_query(
        self, data_source: CompositeQuery[Table]
    ) -> FormattedSubQuery:
        return FormattedSubQuery(_format_query_content(data_source))


def _format_query_content(query: FormattableQuery) -> Sequence[FormattedNode]:
    """
    Produces the content of the formatted query.
    It works for both the composite query and the simple one as the
    only difference is the presence of the prewhere condition.
    Should we have more differences going on we should break this
    method into smaller ones.
    """
    parsing_context = ParsingContext()
    formatter = ClickhouseExpressionFormatter(parsing_context)

    return [
        v
        for v in [
            _format_select(query, formatter),
            PaddingNode("FROM", DataSourceFormatter().visit(query.get_from_clause())),
            _build_optional_string_node("ARRAY JOIN", query.get_arrayjoin(), formatter),
            _build_optional_string_node("PREWHERE", query.get_prewhere_ast(), formatter)
            if isinstance(query, Query)
            else None,
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
            _format_groupby(query, formatter),
            _build_optional_string_node("HAVING", query.get_having(), formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
        ]
        if v is not None
    ]


def _format_select(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> StringNode:
    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns()
    ]
    return StringNode(f"SELECT {', '.join(selected_cols)}")


def _build_optional_string_node(
    name: str,
    expression: Optional[Expression],
    formatter: ClickhouseExpressionFormatter,
) -> Optional[StringNode]:
    return (
        StringNode(f"{name} {expression.accept(formatter)}")
        if expression is not None
        else None
    )


def _format_groupby(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    group_clause: Optional[StringNode] = None
    ast_groupby = query.get_groupby()
    if ast_groupby:
        groupby_expressions = [e.accept(formatter) for e in ast_groupby]
        group_clause_str = f"{', '.join(groupby_expressions)}"
        if query.has_totals():
            group_clause_str = f"{group_clause_str} WITH TOTALS"
        group_clause = StringNode(f"GROUP BY {group_clause_str}")
    return group_clause


def _format_orderby(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_orderby = query.get_orderby()
    if ast_orderby:
        orderby = [
            f"{e.expression.accept(formatter)} {e.direction.value}" for e in ast_orderby
        ]
        return StringNode(f"ORDER BY {', '.join(orderby)}")
    else:
        return None


def _format_limitby(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_limitby = query.get_limitby()

    if ast_limitby is not None:
        return StringNode(
            "LIMIT {} BY {}".format(
                ast_limitby.limit, ast_limitby.expression.accept(formatter)
            )
        )

    return None


def _format_limit(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_limit = query.get_limit()
    return (
        StringNode(f"LIMIT {ast_limit} OFFSET {query.get_offset()}")
        if ast_limit is not None
        else None
    )


class JoinFormatter(JoinVisitor[FormattedNode, Table]):
    """
    Formats a Join tree.
    """

    def visit_individual_node(self, node: IndividualNode[Table]) -> FormattedNode:
        """
        An individual node is formatted as a table name with a table
        alias, thus the padding.
        """
        return PaddingNode(
            None, DataSourceFormatter().visit(node.data_source), node.alias
        )

    def visit_join_clause(self, node: JoinClause[Table]) -> FormattedNode:
        join_type = f"{node.join_type.value} " if node.join_type else ""
        modifier = f"{node.join_modifier.value} " if node.join_modifier else ""
        return SequenceNode(
            [
                node.left_node.accept(self),
                StringNode(f"{modifier}{join_type}JOIN"),
                node.right_node.accept(self),
                StringNode("ON"),
                SequenceNode(
                    [
                        StringNode(
                            f"{k.left.table_alias}.{k.left.column}={k.right.table_alias}.{k.right.column}"
                        )
                        for k in node.keys
                    ],
                    " AND ",
                ),
            ]
        )
