from typing import Any, List, Mapping, MutableMapping, Optional, Sequence, Union

from snuba.query import ProcessableQuery, Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import Entity
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

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


def format_query(
    query: Union[LogicalQuery, CompositeQuery[Entity]]
) -> Mapping[str, Any]:
    """
    Formats a query as a dictionary of clauses. Each expression is either
    represented as a string or as a sequence.

    This representation is meant to be used for tracing/error tracking
    as the query would not be truncated when ingesting the event.
    """

    return {
        **_format_query_body(query),
        "FROM": TracingQueryFormatter().visit(query.get_from_clause()),
    }


class TracingExpressionFormatter(ExpressionVisitor[TExpression]):
    """
    Generates a sequence based representation of an expression to be
    used in tracing as it would not be truncated.
    This looks not too different from the legacy Snuba language
    syntax.
    """

    def __aliasify_str(self, formatted: str, alias: Optional[str]) -> str:
        return formatted if alias is None else f"({formatted} AS {alias})"

    def visit_literal(self, exp: Literal) -> str:
        return self.__aliasify_str(str(exp.value), exp.alias)

    def visit_column(self, exp: Column) -> str:
        return self.__aliasify_str(
            f"{exp.table_name + '.' if exp.table_name else ''}{exp.column_name}",
            exp.alias,
        )

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        return self.__aliasify_str(
            f"{self.visit_column(exp.column)}[{self.visit_literal(exp.key)}]", exp.alias
        )

    def __visit_params(self, params: Sequence[Expression]) -> Sequence[TExpression]:
        return [e.accept(self) for e in params]

    def visit_function_call(self, exp: FunctionCall) -> Sequence[TExpression]:
        ret: List[TExpression] = [exp.alias] if exp.alias else []

        ret.extend([exp.function_name, self.__visit_params(exp.parameters)])
        return ret

    def visit_curried_function_call(
        self, exp: CurriedFunctionCall
    ) -> Sequence[TExpression]:
        ret: List[TExpression] = [exp.alias] if exp.alias else []

        ret.extend(
            [exp.internal_function.accept(self), self.__visit_params(exp.parameters)]
        )
        return ret

    def visit_argument(self, exp: Argument) -> str:
        return self.__aliasify_str(exp.name, exp.alias)

    def visit_lambda(self, exp: Lambda) -> Sequence[TExpression]:
        ret: List[TExpression] = [exp.alias] if exp.alias else []
        ret.extend([list(exp.parameters), exp.transformation.accept(self)])
        return ret


class TracingQueryFormatter(
    DataSourceVisitor[TExpression, Entity], JoinVisitor[TExpression, Entity]
):
    def _visit_simple_source(self, data_source: Entity) -> TExpression:
        ret: MutableMapping[str, Any] = {"ENTITY": data_source.key}
        if data_source.sample is not None:
            ret["SAMPLE"] = str(data_source.sample)
        return ret

    def _visit_join(self, data_source: JoinClause[Entity]) -> TExpression:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Entity]) -> TExpression:
        if isinstance(data_source, LogicalQuery):
            return format_query(data_source)
        else:
            return {}

    def _visit_composite_query(
        self, data_source: CompositeQuery[Entity]
    ) -> TExpression:
        return format_query(data_source)

    def visit_individual_node(self, node: IndividualNode[Entity]) -> TExpression:
        return [node.alias, self.visit(node.data_source)]

    def visit_join_clause(self, node: JoinClause[Entity]) -> TExpression:
        return {
            "LEFT": node.left_node.accept(self),
            "TYPE": node.join_type,
            "RIGHT": node.right_node.accept(self),
            "ON": [
                [
                    f"{c.left.table_alias}.{c.left.column}",
                    f"{c.right.table_alias}.{c.right.column}",
                ]
                for c in node.keys
            ],
        }


def _format_query_body(query: Query) -> Mapping[str, Any]:
    expression_formatter = TracingExpressionFormatter()
    formatted = {
        "SELECT": [
            [e.name, e.expression.accept(expression_formatter)]
            for e in query.get_selected_columns_from_ast()
        ],
        "GROUPBY": [
            e.accept(expression_formatter) for e in query.get_groupby_from_ast()
        ],
        "ORDERBY": [
            [e.expression.accept(expression_formatter), e.direction]
            for e in query.get_orderby_from_ast()
        ],
    }
    array_join = query.get_arrayjoin_from_ast()
    if array_join:
        formatted["ARRAYJOIN"] = array_join.accept(expression_formatter)
    condition = query.get_condition_from_ast()
    if condition:
        formatted["WHERE"] = condition.accept(expression_formatter)
    having = query.get_having_from_ast()
    if having:
        formatted["HAVING"] = having.accept(expression_formatter)
    limitby = query.get_limitby()
    if limitby:
        formatted["LIMITBY"] = {
            "LIMIT": limitby.limit,
            "BY": limitby.expression.accept(expression_formatter),
        }
    limit = query.get_limit()
    if limit:
        formatted["LIMIT"] = limit
    offset = query.get_offset()
    if offset:
        formatted["OFFSET"] = offset
    return formatted
