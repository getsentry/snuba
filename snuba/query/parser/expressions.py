from typing import Any, Iterable, List, Tuple, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.query.expressions import Column, Expression, Literal
from snuba.query.parser.exceptions import ParsingException
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

FUNCTION_NAME_REGEX = r"[a-zA-Z_][a-zA-Z0-9_]*"


minimal_clickhouse_grammar = Grammar(
    rf"""
# This root element is needed because of the ambiguity of the aggregation
# function field which can mean a clickhouse function expression or the simple
# name of a clickhouse function.

root_element          = low_pri_arithmetic space*
low_pri_arithmetic    = space* high_pri_arithmetic (space* low_pri_tuple)*
high_pri_arithmetic   = space* arithmetic_term (space* high_pri_tuple)*
low_pri_tuple         = low_pri_op space* high_pri_arithmetic
high_pri_tuple        = high_pri_op space* arithmetic_term
arithmetic_term       = space* (function_call / numeric_literal / column_name)
low_pri_op            = "+" / "-"
high_pri_op           = "/" / "*"
param_expression      = low_pri_arithmetic / quoted_literal
parameters_list       = parameter* (param_expression)
parameter             = param_expression space* comma space*
function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)?
simple_term           = quoted_literal / numeric_literal / column_name
literal               = ~r"[a-zA-Z0-9_\.:-]+"
quoted_literal        = ~r"((?<!\\)')(.(?!(?<!\\)'))*.?'"
numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
function_name         = ~r"{FUNCTION_NAME_REGEX}"
open_paren            = "("
close_paren           = ")"
space                 = " "
comma                 = ","
"""
)


# parsimonious isn't properly type hinted yet, NodeVisitor has a type of Any
# Add an ignore until parsimonious is properly typed.
class ClickhouseVisitor(NodeVisitor):  # type: ignore
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_root_element(
        self, node: Node, visited_children: Tuple[Expression, Any]
    ) -> Expression:
        ret, _ = visited_children
        return ret

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return visit_function_name(node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return visit_column_name(node, visited_children)

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Any, Expression]
    ) -> LowPriTuple:
        return visit_low_pri_tuple(node, visited_children)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Any, Expression]
    ) -> HighPriTuple:
        return visit_high_pri_tuple(node, visited_children)

    def visit_low_pri_op(self, node: Node, visited_children: Iterable[Any]) -> LowPriOperator:
        return visit_low_pri_op(node, visited_children)

    def visit_high_pri_op(self, node: Node, visited_children: Iterable[Any]) -> HighPriOperator:
        return visit_high_pri_op(node, visited_children)

    def visit_arithmetic_term(
        self, node: Node, visited_children: Tuple[Any, Expression]
    ) -> Expression:
        return visit_arithmetic_term(node, visited_children)

    def visit_low_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, LowPriArithmetic],
    ) -> Expression:
        return visit_low_pri_arithmetic(node, visited_children)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, HighPriArithmetic],
    ) -> Expression:
        return visit_high_pri_arithmetic(node, visited_children)

    def visit_numeric_literal(self, node: Node, visited_children: Iterable[Any]) -> Literal:
        return visit_numeric_literal(node, visited_children)

    def visit_quoted_literal(self, node: Node, visited_children: Tuple[Node]) -> Literal:
        return visit_quoted_literal(node, visited_children)

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
        visited_children: Tuple[str, Any, List[Expression], Any, Union[Node, List[Expression]]],
    ) -> Expression:
        return visit_function_call(node, visited_children)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        return generic_visit(node, visited_children)


def parse_clickhouse_function(function: str) -> Expression:
    try:
        expression_tree = minimal_clickhouse_grammar.parse(function)
    except Exception as cause:
        raise ParsingException(
            f"Cannot parse aggregation {function}", should_report=False
        ) from cause

    return ClickhouseVisitor().visit(expression_tree)  # type: ignore
