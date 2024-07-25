from __future__ import annotations

from typing import Optional, Sequence, Type, Union

from snuba.clickhouse.escaping import escape_alias
from snuba.clickhouse.formatter.expression import (
    ClickhouseExpressionFormatter,
    ExpressionFormatterAnonymized,
    ExpressionFormatterBase,
)
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
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinType,
    JoinVisitor,
)
from snuba.query.data_source.simple import Table
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import Expression, ExpressionVisitor
from snuba.query.parsing import ParsingContext

FormattableQuery = Union[Query, CompositeQuery[Table]]


def format_query(query: FormattableQuery) -> FormattedQuery:
    """
    Formats a Clickhouse Query from the AST representation into an
    intermediate structure that can either be serialized into a string
    (for clickhouse) or extracted as a sequence (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.
    """
    if isinstance(query, Query) and query.is_delete():
        return FormattedQuery(
            _format_delete_query_content(query, ClickhouseExpressionFormatter)
        )
    else:
        return FormattedQuery(
            _format_query_content(query, ClickhouseExpressionFormatter)
        )


def format_query_anonymized(query: FormattableQuery) -> FormattedQuery:
    return FormattedQuery(_format_query_content(query, ExpressionFormatterAnonymized))


class DataSourceFormatter(DataSourceVisitor[FormattedNode, Table]):
    """
    Builds a FormattedNode out of any type of DataSource object. This
    is called when formatting the FROM clause of every nested query
    in a Composite query.
    """

    def __init__(self, expression_formatter_type: Type[ExpressionFormatterBase]):
        self.__expression_formatter_type = expression_formatter_type

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
        return data_source.accept(JoinFormatter(self.__expression_formatter_type))

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Table]
    ) -> FormattedSubQuery:
        assert isinstance(data_source, Query)
        return FormattedSubQuery(
            _format_query_content(data_source, self.__expression_formatter_type),
        )

    def _visit_composite_query(
        self, data_source: CompositeQuery[Table]
    ) -> FormattedSubQuery:
        return FormattedSubQuery(
            _format_query_content(data_source, self.__expression_formatter_type),
        )


def _format_query_content(
    query: FormattableQuery,
    expression_formatter_type: Type[ExpressionFormatterBase],
) -> Sequence[FormattedNode]:
    """
    Produces the content of the formatted query.
    It works for both the composite query and the simple one as the
    only difference is the presence of the prewhere condition.
    Should we have more differences going on we should break this
    method into smaller ones.
    """
    parsing_context = ParsingContext()
    formatter = expression_formatter_type(parsing_context)

    return [
        v
        for v in [
            _format_select(query, formatter),
            PaddingNode(
                "FROM",
                DataSourceFormatter(expression_formatter_type).visit(
                    query.get_from_clause()
                ),
            ),
            _format_arrayjoin(query, formatter),
            (
                _build_optional_string_node(
                    "PREWHERE", query.get_prewhere_ast(), formatter
                )
                if isinstance(query, Query)
                else None
            ),
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
            _format_groupby(query, formatter),
            _build_optional_string_node("HAVING", query.get_having(), formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
        ]
        if v is not None
    ]


def _format_delete_query_content(
    query: FormattableQuery, expression_formatter_type: Type[ExpressionFormatterBase]
) -> Sequence[FormattedNode]:
    formatter = expression_formatter_type()
    return [
        v
        for v in [
            StringNode("DELETE"),
            PaddingNode(
                "FROM",
                DataSourceFormatter(expression_formatter_type).visit(
                    query.get_from_clause()
                ),
            ),
            _format_on_cluster(query, formatter),
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
        ]
        if v is not None
    ]


def _format_on_cluster(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    on_cluster = query.get_on_cluster()
    if on_cluster:
        return StringNode(f"ON CLUSTER {on_cluster.accept(formatter)}")
    return None


def _format_select(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> StringNode:
    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns()
    ]
    return StringNode(f"SELECT {', '.join(selected_cols)}")


def _build_optional_string_node(
    name: str,
    expression: Optional[Expression],
    formatter: ExpressionVisitor[str],
) -> Optional[StringNode]:
    return (
        StringNode(f"{name} {expression.accept(formatter)}")
        if expression is not None
        else None
    )


def _format_groupby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
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


def _format_limitby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_limitby = query.get_limitby()

    if ast_limitby is not None:
        columns_accepted = [column.accept(formatter) for column in ast_limitby.columns]
        return StringNode(
            "LIMIT {} BY {}".format(ast_limitby.limit, ",".join(columns_accepted))
        )

    return None


def _format_arrayjoin(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    array_join = query.get_arrayjoin()
    if array_join is not None:
        column_likes_joined = [el.accept(formatter) for el in array_join]
        return StringNode("ARRAY JOIN {}".format(",".join(column_likes_joined)))

    return None


def _format_limit(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
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

    def __init__(self, ExpressionFormatter: Type[ExpressionFormatterBase]):
        self.ExpressionFormatter = ExpressionFormatter

    def visit_individual_node(self, node: IndividualNode[Table]) -> FormattedNode:
        """
        An individual node is formatted as a table name with a table
        alias, thus the padding.
        """
        return PaddingNode(
            None,
            DataSourceFormatter(self.ExpressionFormatter).visit(node.data_source),
            node.alias,
        )

    def visit_join_clause(self, node: JoinClause[Table]) -> FormattedNode:
        join_type = f"{node.join_type.value} " if node.join_type else ""
        modifier = f"{node.join_modifier.value} " if node.join_modifier else ""
        if node.join_type == JoinType.CROSS:
            return SequenceNode(
                [
                    node.left_node.accept(self),
                    StringNode(f"{modifier}{join_type}JOIN"),
                    node.right_node.accept(self),
                ]
            )
        return SequenceNode(
            [
                node.left_node.accept(self),
                StringNode(f"{modifier}{join_type}JOIN"),
                node.right_node.accept(self),
                StringNode("ON"),
                SequenceNode(
                    [
                        StringNode(
                            f"{k.left.table_alias}.{escape_alias(k.left.column)}={k.right.table_alias}.{escape_alias(k.right.column)}"
                        )
                        for k in node.keys
                    ],
                    " AND ",
                ),
            ]
        )
