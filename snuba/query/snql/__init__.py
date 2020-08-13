from typing import Any, Iterable, List, Sequence, Tuple, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.datasets.dataset import Dataset
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
)
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import OrderBy, OrderByDirection, Query, SelectedExpression
from snuba.query.snql.expression_visitor import (
    HighPriArithmetic,
    HighPriOperator,
    HighPriTuple,
    LowPriArithmetic,
    LowPriOperator,
    LowPriTuple,
    generic_visit,
    visit_arithmetic_term,
    visit_column_name,
    visit_function_call,
    visit_function_name,
    visit_high_pri_arithmetic,
    visit_high_pri_op,
    visit_high_pri_tuple,
    visit_low_pri_arithmetic,
    visit_low_pri_op,
    visit_low_pri_tuple,
    visit_numeric_literal,
    visit_parameter,
    visit_parameters_list,
    visit_quoted_literal,
)

snql_grammar = Grammar(
    fr"""
    query_exp             = match_clause where_clause? collect_clause group_by_clause? having_clause? order_by_clause?

    match_clause          = space* "MATCH" space* open_paren clause close_paren space*
    where_clause          = space* "WHERE" clause space*
    collect_clause        = space* "COLLECT" collect_list space*
    group_by_clause       = space* "BY" group_list space*
    order_by_clause       = space* "ORDER BY" order_list space*
    having_clause         = space* "HAVING" or_expression space*

    condition             = low_pri_arithmetic condition_op (column_name / numeric_literal)
    condition_op          = "=" / "!=" / ">" / ">=" / "<" / "<="

    and_expression        = space* condition space* ("AND" condition)*
    or_expression         = space* and_expression space* ("OR" and_expression)*

    collect_list          = collect_columns* (selected_expression)
    collect_columns       = selected_expression space* comma space*
    selected_expression   = low_pri_arithmetic space*

    group_list            = group_columns* (low_pri_arithmetic)
    group_columns         = low_pri_arithmetic space* comma space*

    order_list            = order_columns* low_pri_arithmetic ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic ("ASC"/"DESC") space* comma space*

    clause                = space* ~r"[-=><\w]+" space*

    low_pri_arithmetic    = space* high_pri_arithmetic space* (low_pri_tuple)*
    high_pri_arithmetic   = space* arithmetic_term space* (high_pri_tuple)*
    low_pri_tuple         = low_pri_op high_pri_arithmetic
    high_pri_tuple        = high_pri_op arithmetic_term
    arithmetic_term       = (space*) (function_call / numeric_literal / column_name) (space*)
    low_pri_op            = "+" / "-"
    high_pri_op           = "/" / "*"
    param_expression      = low_pri_arithmetic / quoted_literal
    parameters_list       = parameter* (param_expression)
    parameter             = param_expression space* comma space*
    function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)?
    simple_term           = quoted_literal / numeric_literal / column_name
    literal               = ~r"[a-zA-Z0-9_\.:-]+"
    quoted_literal        = "'" string_literal "'"
    string_literal        = ~r"[a-zA-Z0-9_\.\+\*\/:-]*"
    numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
    column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
    function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    open_paren            = "("
    close_paren           = ")"
    space                 = " "
    comma                 = ","

"""
)


class SnQLVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_query_exp(self, node: Node, visited_children: Iterable[Any]) -> Query:
        _, _, collect, groupby, having, orderby = visited_children
        # check for empty clauses
        if isinstance(groupby, Node):
            groupby = None
        if isinstance(orderby, Node):
            orderby = None
        if isinstance(having, Node):
            having = None
        return Query({}, None, collect, None, None, None, groupby, having, orderby)

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return visit_function_name(node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return visit_column_name(node, visited_children)

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Expression]
    ) -> LowPriTuple:
        return visit_low_pri_tuple(node, visited_children)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Expression]
    ) -> HighPriTuple:
        return visit_high_pri_tuple(node, visited_children)

    def visit_low_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> LowPriOperator:
        return visit_low_pri_op(node, visited_children)

    def visit_high_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> HighPriOperator:
        return visit_high_pri_op(node, visited_children)

    def visit_arithmetic_term(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        return visit_arithmetic_term(node, visited_children)

    def visit_low_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, LowPriArithmetic],
    ) -> Expression:
        return visit_low_pri_arithmetic(node, visited_children)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, HighPriArithmetic],
    ) -> Expression:
        return visit_high_pri_arithmetic(node, visited_children)

    def visit_numeric_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return visit_numeric_literal(node, visited_children)

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Literal:
        return visit_quoted_literal(node, visited_children)

    def visit_having_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Sequence[OrderBy], Any]
    ) -> Sequence[OrderBy]:
        _, _, conditions, _ = visited_children
        return conditions

    def visit_and_expression(self, node, visited_children):
        _, left_condition, _, and_condition = visited_children
        # in the case of one Condition
        # and_condition will be an empty Node
        if not isinstance(and_condition, Node):
            _, right_condition = and_condition
            return combine_and_conditions([left_condition, right_condition])
        return left_condition

    def visit_or_expression(self, node, visited_children):
        _, left_condition, _, or_condition = visited_children
        # in the case of one Condition
        # or_condition will be an empty Node
        if not isinstance(or_condition, Node):
            _, right_condition = or_condition
            return combine_or_conditions([left_condition, right_condition])
        return left_condition

    def visit_condition_comma(self, node, visited_children):
        bin_condition, _, _, _ = visited_children
        return bin_condition

    def visit_condition(self, node, visited_children):
        exp, op, literal = visited_children
        return binary_condition(None, op, exp, literal)

    def visit_condition_op(self, node, visited_children):
        return OPERATOR_TO_FUNCTION[node.text]

    def visit_order_by_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Sequence[OrderBy], Any]
    ) -> Sequence[OrderBy]:
        _, _, order_columns, _ = visited_children
        return order_columns

    def visit_order_list(
        self, node: Node, visited_children: Tuple[OrderBy, Expression, Node]
    ) -> Sequence[OrderBy]:
        left_order_list, right_order, order = visited_children
        ret: List[OrderBy] = []

        # in the case of one OrderBy
        # left_order_list will be an empty node
        if not isinstance(left_order_list, Node):
            if not isinstance(left_order_list, (list, tuple)):
                ret.append(left_order_list)
            else:
                for p in left_order_list:
                    ret.append(p)

        direction = (
            OrderByDirection.ASC if order.text == "ASC" else OrderByDirection.DESC
        )
        ret.append(OrderBy(direction, right_order))

        return ret

    def visit_order_columns(
        self, node: Node, visited_children: Tuple[Expression, Node, Any, Any, Any]
    ) -> OrderBy:
        column, order, _, _, _ = visited_children

        direction = (
            OrderByDirection.ASC if order.text == "ASC" else OrderByDirection.DESC
        )
        return OrderBy(direction, column)

    def visit_group_by_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Sequence[Expression], Any]
    ) -> Sequence[Expression]:
        _, _, group_columns, _ = visited_children
        return group_columns

    def visit_group_columns(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        columns, _, _, _ = visited_children
        return columns

    def visit_group_list(
        self, node: Node, visited_children: Tuple[Expression, Expression]
    ) -> Sequence[Expression]:
        left_group_list, right_group = visited_children
        ret: List[Expression] = []

        # in the case of one GroupBy / By
        # left_group_list will be an empty node
        if not isinstance(left_group_list, Node):
            if not isinstance(left_group_list, (list, tuple)):
                ret.append(left_group_list)
            else:
                for p in left_group_list:
                    ret.append(p)

        ret.append(right_group)
        return ret

    def visit_collect_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Sequence[SelectedExpression], Any],
    ) -> Sequence[SelectedExpression]:
        _, _, selected_columns, _ = visited_children
        return selected_columns

    def visit_selected_expression(
        self, node: Node, visited_children: Tuple[Expression, Any]
    ) -> SelectedExpression:
        exp, _ = visited_children
        return SelectedExpression(node.text.strip(), exp)

    def visit_collect_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any, Any]
    ) -> SelectedExpression:
        columns, _, _, _, = visited_children
        return columns

    def visit_collect_list(
        self,
        node: Node,
        visited_children: Tuple[SelectedExpression, SelectedExpression],
    ) -> Sequence[SelectedExpression]:
        column_list, right_column = visited_children
        ret: List[SelectedExpression] = []

        # in the case of one Collect
        # column_list will be an empty node
        if not isinstance(column_list, Node):
            if not isinstance(column_list, (list, tuple)):
                ret.append(column_list)
            else:
                for p in column_list:
                    ret.append(p)

        ret.append(right_column)
        return ret

    def visit_parameter(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        return visit_parameter(node, visited_children)

    def visit_parameters_list(
        self,
        node: Node,
        visited_children: Tuple[Union[Expression, List[Expression]], Expression],
    ) -> List[Expression]:
        return visit_parameters_list(node, visited_children)

    def visit_function_call(
        self,
        node: Node,
        visited_children: Tuple[
            str, Any, List[Expression], Any, Union[Node, List[Expression]]
        ],
    ) -> Expression:
        return visit_function_call(node, visited_children)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        return generic_visit(node, visited_children)


def parse_snql_query(body: str, dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    exp_tree = snql_grammar.parse(body)
    return SnQLVisitor().visit(exp_tree)
