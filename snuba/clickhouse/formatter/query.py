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
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source import DataSource
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Table
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings

FormattableQuery = Union[Query, CompositeQuery[Table]]


def format_query(query: FormattableQuery, settings: RequestSettings) -> FormattedQuery:
    """
    Formats a Clickhouse Query.

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
        return FormattedQuery(format_single_query_content(query))
    else:
        return FormattedQuery(format_composite_query_content(query))


def format_data_source(data_source: DataSource) -> FormattedNode:
    """
    Builds a FormattedNode out of any type of DataSource object. This
    is called when formatting the FROM clause of every nested query
    in a Composite query.
    """
    if isinstance(data_source, Table):
        return format_table(data_source)
    elif isinstance(data_source, Query):
        return FormattedSubQuery(format_single_query_content(data_source))
    elif isinstance(data_source, JoinClause):
        return data_source.accept(JoinFormatter())
    elif isinstance(data_source, CompositeQuery):
        return FormattedSubQuery(format_composite_query_content(data_source))
    else:
        raise NotImplementedError(f"Unsupported data source {type(data_source)}")


def format_table(data_source: Table) -> StringNode:
    final = " FINAL" if data_source.final else ""
    sample = f" SAMPLE {data_source.sampling_rate}" if data_source.sampling_rate else ""
    return StringNode(f"{data_source.table_name}{final}{sample}")


def _format_select(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> StringNode:
    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns_from_ast()
    ]
    return StringNode(f"SELECT {', '.join(selected_cols)}")


def _format_arrayjoin(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_arrayjoin = query.get_arrayjoin_from_ast()
    return (
        StringNode(f"ARRAY JOIN {ast_arrayjoin.accept(formatter)}")
        if ast_arrayjoin is not None
        else None
    )


def _format_prewhere(
    query: Query, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_prewhere = query.get_prewhere_ast()
    return (
        StringNode(f"PREWHERE {ast_prewhere.accept(formatter)}")
        if ast_prewhere is not None
        else None
    )


def _format_condition(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_condition = query.get_condition_from_ast()
    return (
        StringNode(f"WHERE {ast_condition.accept(formatter)}")
        if ast_condition is not None
        else None
    )


def _format_groupby(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    group_clause: Optional[StringNode] = None
    ast_groupby = query.get_groupby_from_ast()
    if ast_groupby:
        # reformat to use aliases generated during the select clause formatting.
        groupby_expressions = [e.accept(formatter) for e in ast_groupby]
        group_clause_str = f"({', '.join(groupby_expressions)})"
        if query.has_totals():
            group_clause_str = f"{group_clause_str} WITH TOTALS"
        group_clause = StringNode(f"GROUP BY {group_clause_str}")
    return group_clause


def _format_having(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_having = query.get_having_from_ast()
    return (
        StringNode(f"HAVING {ast_having.accept(formatter)}")
        if ast_having is not None
        else None
    )


def _format_orderby(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_orderby = query.get_orderby_from_ast()
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
    return (
        StringNode("LIMIT {} BY {}".format(*ast_limitby))
        if ast_limitby is not None
        else None
    )


def _format_limit(
    query: AbstractQuery, formatter: ClickhouseExpressionFormatter
) -> Optional[StringNode]:
    ast_limit = query.get_limit()
    return (
        StringNode(f"LIMIT {ast_limit} OFFSET {query.get_offset()}")
        if ast_limit is not None
        else None
    )


def format_single_query_content(query: Query) -> Sequence[FormattedNode]:
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

    return [
        value
        for value in [
            _format_select(query, formatter),
            PaddingNode("FROM", format_data_source(query.get_from_clause())),
            _format_arrayjoin(query, formatter),
            _format_prewhere(query, formatter),
            _format_condition(query, formatter),
            _format_groupby(query, formatter),
            _format_having(query, formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
        ]
        if value
    ]


def format_composite_query_content(
    query: CompositeQuery[Table],
) -> Sequence[FormattedNode]:
    parsing_context = ParsingContext()
    formatter = ClickhouseExpressionFormatter(parsing_context)

    return [
        value
        for value in [
            _format_select(query, formatter),
            PaddingNode("FROM", format_data_source(query.get_from_clause())),
            _format_arrayjoin(query, formatter),
            _format_condition(query, formatter),
            _format_groupby(query, formatter),
            _format_having(query, formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
        ]
        if value
    ]


class JoinFormatter(JoinVisitor[FormattedNode, Table]):
    """
    Formats a Join tree.
    """

    def visit_individual_node(self, node: IndividualNode[Table]) -> FormattedNode:
        """
        An individual node is formatted as a table name with a table
        alias, thus the padding.
        """
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
