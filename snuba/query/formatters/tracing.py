import textwrap
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
    StringifyVisitor,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


# def format_query(
#     query: Union[LogicalQuery, CompositeQuery[Entity]]
# ) -> Mapping[str, Any]:
#     """
#     Formats a query as a dictionary of clauses. Each expression is either
#     represented as a string or as a sequence.
#
#     This representation is meant to be used for tracing/error tracking
#     as the query would not be truncated when ingesting the event.
#     """
#
#     return {
#         **_format_query_body(query),
#         "FROM": TracingQueryFormatter().visit(query.get_from_clause()),
#     }


def _indent_str_list(str_list: List[str], levels: int) -> List[str]:
    indent = "  " * levels
    return [f"{indent}{s}" for s in str_list]


def format_query(query: Union[LogicalQuery, CompositeQuery[Entity]]) -> List[str]:
    """
    Formats a query as a dictionary of clauses. Each expression is either
    represented as a string or as a sequence.

    This representation is meant to be used for tracing/error tracking
    as the query would not be truncated when ingesting the event.
    """
    return [
        *_format_query_body(query),
        "FROM",
        *_indent_str_list(TracingQueryFormatter().visit(query.get_from_clause()), 1)
        # f"FROM {eformatter.visit(query.get_from_clause())}",
    ]


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
    DataSourceVisitor[List[str], Entity], JoinVisitor[List[str], Entity]
):
    def __init__(self, initial_indent: int = 0):
        super().__init__()
        self._initial_indent = initial_indent
        self._indent_level = 0

    def _indent_str_list(self, str_list: List[str], levels: int) -> List[str]:
        indent = "  " * levels
        return [f"{indent}{s}" for s in str_list]

    def _visit_simple_source(self, data_source: Entity) -> List[str]:
        ret: MutableMapping[str, Any] = {"ENTITY": data_source.key}
        if data_source.sample is not None:
            ret["SAMPLE"] = str(data_source.sample)
        return [f"ENTITY {data_source.key} SAMPLE {data_source.sample}"]

    def _visit_join(self, data_source: JoinClause[Entity]) -> List[str]:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(self, data_source: ProcessableQuery[Entity]) -> List[str]:
        if isinstance(data_source, LogicalQuery):
            return format_query(data_source)
        else:
            return []

    def _visit_composite_query(self, data_source: CompositeQuery[Entity]) -> List[str]:
        return format_query(data_source)

    def visit_individual_node(self, node: IndividualNode[Entity]) -> List[str]:
        return [f"{self.visit(node.data_source)} AS `{node.alias}`"]

    def visit_join_clause(self, node: JoinClause[Entity]) -> List[str]:
        # There is only one of these in the on clause (I think)
        on_list = [
            [
                f"{c.left.table_alias}.{c.left.column}",
                f"{c.right.table_alias}.{c.right.column}",
            ]
            for c in node.keys
        ][0]

        return [
            *_indent_str_list(node.left_node.accept(self), 1),
            f"{node.join_type.name.upper()} JOIN",
            *_indent_str_list(node.right_node.accept(self), 1),
            "ON",
            *_indent_str_list(on_list, 1,),
        ]


def _format_query_body(query: Query) -> List[str]:
    eformatter = StringifyVisitor(level=0, initial_indent=1)

    selects = "\n  ".join(
        [
            f"{e.name},{e.expression.accept(eformatter)}"
            for e in query.get_selected_columns()
        ]
    )
    select_str = f"SELECT\n  {selects}" if selects else ""

    groupbys = "\n  ".join([e.accept(eformatter) for e in query.get_groupby()])
    groupby_str = f"GROUPBY\n  {groupbys}" if groupbys else ""

    orderbys = "\n".join(
        [
            f"{e.expression.accept(eformatter)} {e.direction.value}"
            for e in query.get_orderby()
        ]
    )
    orderby_str = f"ORDER_BY\n  {orderbys}" if orderbys else ""

    str_list = [select_str, groupby_str, orderby_str]

    array_join = query.get_arrayjoin()
    if array_join:
        str_list.append(f"  ARRAYJOIN\n  {array_join.accept(eformatter)}")
    condition = query.get_condition()
    if condition:
        str_list.append(f"  WHERE\n  {condition.accept(eformatter)}")
    having = query.get_having()
    if having:
        str_list.append(f"  HAVING\n  {having.accept(eformatter)}")
    limitby = query.get_limitby()
    if limitby:
        str_list.append(
            f"  LIMIT {limitby.limit}\n  BY {limitby.expression.accept(eformatter)}"
        )
    limit = query.get_limit()
    if limit:
        str_list.append(f"  LIMIT {limit}")
    offset = query.get_offset()
    if offset:
        str_list.append(f"  OFFSET {offset}")
    return str_list
