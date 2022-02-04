from typing import Optional, Sequence, Type, Union

from snuba.clickhouse.formatter.expression import ExpressionFormatterAnonymized
from snuba.clickhouse.formatter.nodes import (
    FormattedNode,
    FormattedQuery,
    FormattedSubQuery,
    PaddingNode,
    StringNode,
)
from snuba.clickhouse.formatter.query import (
    _build_optional_string_node,
    _format_arrayjoin,
    _format_groupby,
    _format_limit,
    _format_limitby,
    _format_orderby,
    _format_select,
)
from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import ExpressionVisitor
from snuba.query.logical import Query
from snuba.query.logical import Query as LogicalQuery


def format_snql_anonymized(
    query: Union[LogicalQuery, CompositeQuery[Entity]]
) -> FormattedQuery:

    return FormattedQuery(_format_query_content(query, ExpressionFormatterAnonymized))


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
            _format_arrayjoin(query, formatter),
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
            _build_optional_string_node("HAVING", query.get_having(), formatter),
            _format_orderby(query, formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
            _format_granularity(query, formatter),
        ]
        if v is not None
    ]


def _format_granularity(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_granularity = query.get_granularity()
    return (
        StringNode(f"GRANULARITY {ast_granularity}")
        if ast_granularity is not None
        else None
    )
