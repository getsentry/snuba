from typing import Any, Iterable, List, Tuple, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.datasets.dataset import Dataset
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
    query_exp             = match_clause where_clause? collect_clause? group_by_clause? having_clause? order_by_clause?

    match_clause          = space* "MATCH" space* open_paren clause close_paren space*
    where_clause          = space* "WHERE" clause space*
    collect_clause        = space* "COLLECT" collect_list space*
    group_by_clause       = space* "BY" group_list space*
    order_by_clause       = space* "ORDER BY" order_list space*

    collect_list          = collect_columns* (low_pri_arithmetic)
    collect_columns       = low_pri_arithmetic space* comma space*

    group_list            = group_columns* (low_pri_arithmetic)
    group_columns         = low_pri_arithmetic space* comma space*

    order_list            = order_columns* low_pri_arithmetic ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic ("ASC"/"DESC") space* comma space*

    having_clause         = space* "HAVING" clause space*
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

    def visit_query_exp(self, node, visited_children):
        _, _, collect, groupby, _, orderby = visited_children
        return Query({}, None, collect, None, None, None, groupby, None, orderby)

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

    def visit_order_by_clause(self, node, visited_children):
        _, _, order_columns, _ = visited_children
        return order_columns

    def visit_order_list(self, node, visited_children):
        left_order_list, right_order, order = visited_children

        ret: List[OrderBy] = []
        if not isinstance(left_order_list, Node):
            if not isinstance(left_order_list, (list, tuple)):
                ret.append(left_order_list)
            else:
                for p in left_order_list:
                    ret.append(p)

        if order.text == "ASC":
            ret.append(OrderBy(OrderByDirection.ASC, right_order))
        elif order.text == "DESC":
            ret.append(OrderBy(OrderByDirection.DESC, right_order))

        return ret

    def visit_order_columns(self, node, visited_children):
        column, order, _, _, _ = visited_children

        if order.text == "ASC":
            order_object = OrderBy(OrderByDirection.ASC, column)
        elif order.text == "DESC":
            order_object = OrderBy(OrderByDirection.DESC, column)

        return order_object

    def visit_group_by_clause(self, node, visited_children):
        _, _, group_columns, _ = visited_children
        return group_columns

    def visit_group_columns(self, node, visited_children):
        columns, _, _, _ = visited_children
        return columns

    def visit_group_list(self, node, visited_children):
        left_group_list, right_group = visited_children
        ret: List[Expression] = []
        if not isinstance(left_group_list, Node):
            if not isinstance(left_group_list, (list, tuple)):
                ret.append(left_group_list)
            else:
                for p in left_group_list:
                    ret.append(p)

        ret.append(right_group)
        return ret

    def visit_collect_clause(self, node, visited_children):
        _, _, selected_columns, _ = visited_children
        return selected_columns

    def visit_collect_columns(self, node, visited_children):
        columns, _, _, _, = visited_children

        if columns.alias is None:
            text = node.text.split(",")[0].strip()
            return SelectedExpression(text, columns)
        else:
            return SelectedExpression(columns.alias, columns)

    def visit_collect_list(self, node, visited_children):
        column_list, right_column = visited_children
        ret: List[SelectedExpression] = []
        if not isinstance(column_list, Node):
            if not isinstance(column_list, (list, tuple)):
                ret.append(column_list)
            else:
                for p in column_list:
                    ret.append(p)

        right_text = node.text.split(",")[-1].strip()
        ret.append(SelectedExpression(right_text, right_column))
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
