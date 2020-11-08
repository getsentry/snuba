from dataclasses import replace
from typing import Optional

from snuba import settings as snuba_settings
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.formatter.nodes import (
    FormattedNode,
    FormattedQuery,
    PaddingNode,
    SequenceNode,
    StringNode,
)
from snuba.clickhouse.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source import DataSource
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings


def format_query(query: Query, settings: RequestSettings) -> FormattedQuery:
    """
    Formats a Clickhouse Query.

    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """

    if settings.get_turbo() and not query.get_from_clause().sampling_rate:
        query.set_from_clause(
            replace(
                query.get_from_clause(), sampling_rate=snuba_settings.TURBO_SAMPLE_RATE
            )
        )

    return format_simple_query(query)


def format_data_source(data_source: DataSource) -> FormattedNode:
    """
    Builds a FormattedNode out of any type of DataSource object. This
    is called when formatting the FROM clause of every nested query
    in a Composite query.
    """
    if isinstance(data_source, Table):
        return format_table(data_source)
    elif isinstance(data_source, Query):
        return format_simple_query(data_source)
    elif isinstance(data_source, JoinClause):
        return data_source.accept(JoinFormatter())
    elif isinstance(data_source, CompositeQuery):
        raise NotImplementedError("Composite queries not yet implemented")
    else:
        raise NotImplementedError(f"Unsupported data source {type(data_source)}")


def format_table(data_source: Table) -> StringNode:
    final = " FINAL" if data_source.final else ""
    sample = f" SAMPLE {data_source.sampling_rate}" if data_source.sampling_rate else ""
    return StringNode(f"{data_source.table_name}{final}{sample}")


def format_simple_query(query: Query) -> FormattedQuery:
    """
    Formats a Clickhouse Query from the AST representation into an
    intermediate structure that can either be serialized into a string
    (for clickhouse) or extracted as a sequence (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.

    Remark: The ideal way to structure this would be a visitor just
    like we did for the AST expression but this is not possible
    without introducing either circular dependencies or dropping
    type checking since the visitor and the classes to visit cannot
    be in the same module as of now and in a visitor pattern they would
    have to be mutually dependent.
    """
    parsing_context = ParsingContext()
    formatter = ClickhouseExpressionFormatter(parsing_context)

    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns_from_ast()
    ]
    select_clause = StringNode(f"SELECT {', '.join(selected_cols)}")

    from_clause = PaddingNode("FROM", format_data_source(query.get_from_clause()))

    ast_arrayjoin = query.get_arrayjoin_from_ast()
    array_join_clause = (
        StringNode(f"ARRAY JOIN {ast_arrayjoin.accept(formatter)}")
        if ast_arrayjoin is not None
        else None
    )

    ast_prewhere = query.get_prewhere_ast()
    prewhere_clause = (
        StringNode(f"PREWHERE {ast_prewhere.accept(formatter)}")
        if ast_prewhere is not None
        else None
    )

    ast_condition = query.get_condition_from_ast()
    where_clause = (
        StringNode(f"WHERE {ast_condition.accept(formatter)}")
        if ast_condition is not None
        else None
    )

    group_clause: Optional[FormattedNode] = None
    ast_groupby = query.get_groupby_from_ast()
    if ast_groupby:
        # reformat to use aliases generated during the select clause formatting.
        groupby_expressions = [e.accept(formatter) for e in ast_groupby]
        group_clause_str = f"({', '.join(groupby_expressions)})"
        if query.has_totals():
            group_clause_str = f"{group_clause_str} WITH TOTALS"
        group_clause = StringNode(f"GROUP BY {group_clause_str}")

    ast_having = query.get_having_from_ast()
    having_clause = (
        StringNode(f"HAVING {ast_having.accept(formatter)}")
        if ast_having is not None
        else None
    )

    ast_orderby = query.get_orderby_from_ast()
    if ast_orderby:
        orderby = [
            f"{e.expression.accept(formatter)} {e.direction.value}" for e in ast_orderby
        ]
        order_clause: Optional[FormattedNode] = StringNode(
            f"ORDER BY {', '.join(orderby)}"
        )
    else:
        order_clause = None

    ast_limitby = query.get_limitby()
    limitby_clause = (
        StringNode("LIMIT {} BY {}".format(*ast_limitby))
        if ast_limitby is not None
        else None
    )

    ast_limit = query.get_limit()
    limit_clause = (
        StringNode(f"LIMIT {ast_limit} OFFSET {query.get_offset()}")
        if ast_limit is not None
        else None
    )

    return FormattedQuery(
        [
            value
            for value in [
                select_clause,
                from_clause,
                array_join_clause,
                prewhere_clause,
                where_clause,
                group_clause,
                having_clause,
                order_clause,
                limitby_clause,
                limit_clause,
            ]
            if value
        ]
    )


class JoinFormatter(JoinVisitor[FormattedNode, Table]):
    """
    Formats a Join tree.
    """

    def visit_individual_node(self, node: IndividualNode[Table]) -> FormattedNode:
        return PaddingNode(None, format_data_source(node.data_source), node.alias)

    def visit_join_clause(self, node: JoinClause[Table]) -> FormattedNode:
        join_type = f"{node.join_type.value} " if node.join_type else ""
        modifier = f"{node.join_modifier.value} " if node.join_modifier else ""
        return SequenceNode(
            [
                node.left_node.accept(self),
                StringNode(f"{join_type}{modifier}JOIN"),
                node.right_node.accept(self),
                StringNode("ON"),
                SequenceNode(
                    [
                        StringNode(
                            f"{k.left.table_alias}.{k.left.column}={k.right.table_alias}.{k.right.column}"
                        )
                        for k in node.keys
                    ]
                ),
            ]
        )
