from typing import (
    Any,
    Iterable,
    List,
    Tuple,
    Union,
)

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.datasets.dataset import Dataset
from snuba.query.expressions import (
    Column,
    Expression,
    Literal,
)
from snuba.query.logical import Query, SelectedExpression


from snuba.query.snql.expression_visitor import (
    LowPriOperator,
    HighPriOperator,
    LowPriTuple,
    HighPriTuple,
    LowPriArithmetic,
    HighPriArithmetic,
    exp_visit_function_name,
    exp_visit_column_name,
    exp_visit_low_pri_tuple,
    exp_visit_high_pri_tuple,
    exp_visit_low_pri_op,
    exp_visit_high_pri_op,
    exp_visit_parameter,
    exp_visit_parameters_list,
    exp_visit_function_call,
    exp_visit_quoted_literal,
    exp_generic_visit,
    exp_visit_high_pri_arithmetic,
    exp_visit_low_pri_arithmetic,
    exp_visit_arithmetic_term,
    exp_visit_numeric_literal,
)


snql_grammar = Grammar(
    fr"""
    query_exp             = match_clause? where_clause? collect_clause? having_clause? order_by_clause?
    match_clause          = space* "MATCH" space* "(" clause ")" space*
    where_clause          = space* "WHERE" clause space*
    collect_clause        = space* "COLLECT" collect_list space*
    order_by_clause       = space* "ORDER BY" collect_list ("ASC"/"DESC") space*

    collect_list          = collect_columns* (low_pri_arithmetic)
    collect_columns       = low_pri_arithmetic space* comma space*

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
        _, _, collect, _, _ = visited_children

        return Query({}, None, collect, None,)

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return exp_visit_function_name(self, node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return exp_visit_column_name(self, node, visited_children)

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Expression]
    ) -> LowPriTuple:
        return exp_visit_low_pri_tuple(self, node, visited_children)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Expression]
    ) -> HighPriTuple:
        return exp_visit_high_pri_tuple(self, node, visited_children)

    def visit_low_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> LowPriOperator:
        return exp_visit_low_pri_op(self, node, visited_children)

    def visit_high_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> HighPriOperator:
        return exp_visit_high_pri_op(self, node, visited_children)

    def visit_arithmetic_term(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        return exp_visit_arithmetic_term(self, node, visited_children)

    def visit_low_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, LowPriArithmetic],
    ) -> Expression:
        return exp_visit_low_pri_arithmetic(self, node, visited_children)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, HighPriArithmetic],
    ) -> Expression:
        return exp_visit_high_pri_arithmetic(self, node, visited_children)

    def visit_numeric_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return exp_visit_numeric_literal(self, node, visited_children)

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Literal:
        return exp_visit_quoted_literal(self, node, visited_children)

    def visit_collect_clause(self, node, visited_children):
        _, _, selected_columns, _ = visited_children
        return selected_columns

    def visit_collect_columns(self, node, visited_children):
        columns, _, _, _, = visited_children
        return columns

    def visit_collect_list(self, node, visited_children):
        column_list, right_column = visited_children
        ret: List[SelectedExpression] = []
        if not isinstance(column_list, Node):
            if not isinstance(column_list, (list, tuple)):
                ret.append(SelectedExpression(None, column_list))
            else:
                for p in column_list:
                    ret.append(SelectedExpression(None, p))

        ret.append(SelectedExpression(None, right_column))
        return ret

    def visit_parameter(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        return exp_visit_parameter(self, node, visited_children)

    def visit_parameters_list(
        self,
        node: Node,
        visited_children: Tuple[Union[Expression, List[Expression]], Expression],
    ) -> List[Expression]:
        return exp_visit_parameters_list(self, node, visited_children)

    def visit_function_call(
        self,
        node: Node,
        visited_children: Tuple[
            str, Any, List[Expression], Any, Union[Node, List[Expression]]
        ],
    ) -> Expression:
        return exp_visit_function_call(self, node, visited_children)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        return exp_generic_visit(self, node, visited_children)


exp_tree = snql_grammar.parse("COLLECT 4-5, 3*g(c), c")
parsed_exp = SnQLVisitor().visit(exp_tree)
print(parsed_exp.get_selected_columns_from_ast())


def parse_snql_query(body: str, dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """

    return Query({}, None,)
