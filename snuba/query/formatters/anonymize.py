from datetime import date, datetime
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
from snuba.query.data_source.simple import Entity, SimpleDataSource
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery


def format_query_anonymized(
    query: Union[LogicalQuery, CompositeQuery[Entity]]
) -> FormattedQuery:
    """
    Formats a Snuba Query from the AST representation into an
    intermediate structure that can either be serialized into a string
    (for clickhouse) or extracted as a sequence (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.
    """
    return FormattedQuery(_format_query_content(query, AnonymizedStringifyVisitor))


class AnonymizedStringifyVisitor(ExpressionVisitor[str]):
    """Visitor implementation to turn an expression
    into a string format with anonymized literals
    Usage:
        # Any expression class supported by the visitor will do
        >>> exp: Expression = Expression()
        >>> visitor = StringifyVisitor()
        >>> exp_str = exp.accept(visitor)
    """

    def _get_alias_str(self, exp: Expression) -> str:
        # Every expression has an optional alias so we handle that here
        return f" AS `{exp.alias}`" if exp.alias else ""

    def visit_literal(self, exp: Literal) -> str:
        if exp.value is None:
            return "NULL"
        if isinstance(exp.value, str):
            return "$S"
        elif isinstance(exp.value, datetime):
            return "$DT"
        elif isinstance(exp.value, date):
            return "$D"
        elif isinstance(exp.value, bool):
            return "$B"
        elif isinstance(exp.value, (int, float)):
            return "$N"
        else:
            raise ValueError(f"Unexpected literal type {type(exp.value)}")

    def visit_column(self, exp: Column) -> str:
        column_str = (
            f"{exp.table_name}.{exp.column_name}"
            if exp.table_name
            else f"{exp.column_name}"
        )
        return f"{column_str}{self._get_alias_str(exp)}"

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        # we want to visit the literal node to format it properly
        # but for the subscritable reference we don't need it to
        # be indented or newlined. Hence we remove the prefix
        # from the string
        literal_str = exp.key.accept(self)

        # if the subscripted column is aliased, we wrap it with parens to make life
        # easier for the viewer
        column_str = (
            f"({exp.column.accept(self)})"
            if exp.column.alias is not None
            else f"{exp.column.accept(self)}"
        )

        # this line will already have the necessary prefix due to the visit_column
        # function
        subscripted_column_str = f"{column_str}[{literal_str}]"
        # after we know that, all we need to do as add the alias
        return f"{subscripted_column_str}{self._get_alias_str(exp)}"

    def visit_function_call(self, exp: FunctionCall) -> str:
        param_str = ",".join([f" {param.accept(self)}" for param in exp.parameters])
        return f"{exp.function_name}({param_str} ){self._get_alias_str(exp)}"

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        param_str = ",".join([f" {param.accept(self)}" for param in exp.parameters])
        # The internal function repr will already have the
        # prefix appropriate for the level, we don't need to
        # insert it here
        return f"{exp.internal_function.accept(self)}({param_str} ){self._get_alias_str(exp)}"

    def visit_argument(self, exp: Argument) -> str:
        return f"{exp.name}{self._get_alias_str(exp)}"

    def visit_lambda(self, exp: Lambda) -> str:
        params_str = ",".join(exp.parameters)
        transformation_str = exp.transformation.accept(self)
        return f"({params_str} -> {transformation_str} ){self._get_alias_str(exp)}"


class StringQueryFormatter(
    DataSourceVisitor[FormattedNode, Entity], JoinVisitor[FormattedNode, Entity]
):
    def __init__(self, expression_formatter_type: Type[ExpressionVisitor[str]]):
        self.__expression_formatter_type = expression_formatter_type

    def _visit_simple_source(self, data_source: SimpleDataSource) -> StringNode:
        sample_val = getattr(
            data_source, "sample", getattr(data_source, "sampling_rate", None)
        )
        sample_str = f" SAMPLE {sample_val}" if sample_val is not None else ""
        return StringNode(f"{data_source.human_readable_id}{sample_str}")

    def _visit_join(self, data_source: JoinClause[Entity]) -> StringNode:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[Entity]
    ) -> FormattedSubQuery:
        assert isinstance(data_source, LogicalQuery)
        return FormattedSubQuery(
            _format_query_content(data_source, self.__expression_formatter_type)
        )

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> FormattedSubQuery:
        return FormattedSubQuery(
            _format_query_content(data_source, self.__expression_formatter_type)
        )

    def visit_individual_node(self, node: IndividualNode[Entity]) -> StringNode:
        return StringNode(f"{node.alias}, {self.visit(node.data_source)}")

    def visit_join_clause(self, node: JoinClause[Entity]) -> StringNode:
        left = f"LEFT {node.left_node.accept(self)}"
        type = f"TYPE {node.join_type}"
        right = f"RIGHT {node.right_node.accept(self)}"
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
            _format_select(query, formatter),
            _format_groupby(query, formatter),
            _format_orderby(query, formatter),
            _build_optional_string_node("ARRAY JOIN", query.get_arrayjoin(), formatter),
            _build_optional_string_node("WHERE", query.get_condition(), formatter),
            _build_optional_string_node("HAVING", query.get_having(), formatter),
            _format_limitby(query, formatter),
            _format_limit(query, formatter),
            _format_offset(query, formatter),
            PaddingNode(
                "FROM",
                StringQueryFormatter(expression_formatter_type).visit(
                    query.get_from_clause()
                ),
            ),
        ]
        if v is not None
    ]


def _format_select(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> StringNode:
    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns()
    ]
    print(type(selected_cols[0]))
    return StringNode(f"SELECT {', '.join(selected_cols)}")


def _format_groupby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_groupby = query.get_groupby()
    if ast_groupby:
        selected_cols = [e.accept(formatter) for e in ast_groupby]
        return StringNode(f"GROUPBY {', '.join(selected_cols)}")
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


def _format_limitby(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
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
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    ast_limit = query.get_limit()
    return StringNode(f"LIMIT {ast_limit}") if ast_limit is not None else None


def _format_offset(
    query: AbstractQuery, formatter: ExpressionVisitor[str]
) -> Optional[StringNode]:
    offset = query.get_offset()
    return StringNode(f"OFFSET {query.get_offset()}") if offset is not None else None
