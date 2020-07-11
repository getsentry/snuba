from dataclasses import replace
from typing import List, Tuple, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor
from typing import Any, Iterable, Optional

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.parser.functions import parse_function_to_expr
from snuba.query.parser.strings import parse_string_to_expr
from snuba.util import is_function
from snuba.query.dsl import plus, multiply


minimal_clickhouse_grammar = Grammar(
    r"""
# This root element is needed because of the ambiguity of the aggregation
# function field which can mean a clickhouse function expression or the simple
# name of a clickhouse function.
root_element          = function_call / function_name
expression            = function_call / arithmetic_expression / simple_term
arithmetic_expression = simple_term2 (("+" arithmetic_expression) / empty)
parameters_list       = parameter* (expression)
parameter             = expression space* comma space*
function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)?
simple_term           = quoted_literal / numeric_literal / column_name
simple_term2          = numeric_literal (("*" simple_term2) / empty)
literal               = ~r"[a-zA-Z0-9_\.:-]+"
quoted_literal        = "'" string_literal "'"
string_literal        = ~r"[a-zA-Z0-9_\.:-]*"
numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
open_paren            = "("
close_paren           = ")"
space                 = " "
comma                 = ","
empty                 = ''
"""
)


class ClickhouseVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return Column(None, None, node.text)

    def visit_empty(self, node, children):
        return

    def visit_simple_term2(self, node, children):
        factor, term = children

        if term is None:
            # A check for any None child nodes
            # that arose from empty space ''
            return factor
        else:
            return multiply(factor, term[1])

    def visit_arithmetic_expression(self, node, children):
        term, exp = children

        if exp is None:
            return term
        else:
            return plus(term, exp[1])

    def visit_numeric_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        try:
            return Literal(None, int(node.text))
        except Exception:
            return Literal(None, float(node.text))

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Literal:
        _, val, _ = visited_children
        return Literal(None, val.text)

    def visit_parameter(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        param, _, _, _, = visited_children
        return param

    def visit_parameters_list(
        self,
        node: Node,
        visited_children: Tuple[Union[Expression, List[Expression]], Expression],
    ) -> List[Expression]:
        left_section, right_section = visited_children
        ret: List[Expression] = []
        if not isinstance(left_section, Node):
            # We get a Node when the parameter rule is empty. Thus
            # no parameters
            if not isinstance(left_section, (list, tuple)):
                # This can happen when there is one parameter only
                # thus the generic visitor method removes the list.
                ret = [left_section]
            else:
                ret = [p for p in left_section]
        ret.append(right_section)
        return ret

    def visit_function_call(
        self,
        node: Node,
        visited_children: Tuple[
            str, Any, List[Expression], Any, Union[Node, List[Expression]]
        ],
    ) -> Expression:
        name, _, params1, _, params2 = visited_children
        param_list1 = tuple(params1)
        internal_f = FunctionCall(None, name, param_list1)
        if isinstance(params2, Node) and params2.text == "":
            # params2.text == "" means empty node.
            return internal_f

        _, param_list2, _ = params2
        if isinstance(param_list2, (list, tuple)) and len(param_list2) > 0:
            param_list2 = tuple(param_list2)
        else:
            # This happens when the second parameter list is empty. Somehow
            # it does not turn into an empty list.
            param_list2 = ()
        return CurriedFunctionCall(None, internal_f, param_list2)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        if isinstance(visited_children, list) and len(visited_children) == 1:
            # This is to remove the dependency of the visitor correctness on the
            # structure of the grammar. Every rule that does not have a visitor method
            # (not all are needed) wraps the children into a list before returning
            # to the parent. The result is that the function call rule needs to unpack
            # a very nested list to get to columns. Which makes the visitor very fragile.
            # This way the nesting simply does not happen  and the visitor works
            # even if we add nesting levels.
            return visited_children[0]
        return visited_children or node


def parse_expression(val: Any) -> Expression:
    """
    Parse a simple or structured expression encoded in the Snuba query language
    into an AST Expression.
    """
    if is_function(val, 0):
        return parse_function_to_expr(val)
    if isinstance(val, str):
        return parse_string_to_expr(val)
    raise ValueError(f"Expression to parse can only be a function or a string: {val}")


def parse_aggregation(
    aggregation_function: str, column: Any, alias: Optional[str]
) -> Expression:
    """
    Aggregations, unfortunately, support both Snuba syntax and a subset
    of Clickhouse syntax. In order to preserve this behavior and still build
    a meaningful AST when parsing the query, we need to do some parsing of
    the clickhouse expression. (not that we should support this, but it is
    used in production).
    """
    expression_tree = minimal_clickhouse_grammar.parse(aggregation_function)
    parsed_expression = ClickhouseVisitor().visit(expression_tree)

    if not isinstance(column, (list, tuple)):
        columns: Iterable[Any] = (column,)
    else:
        columns = column

    columns_expr = [parse_expression(column) for column in columns if column]

    if isinstance(parsed_expression, str):
        # Simple aggregation with snuba syntax ["count", ["c1", "c2"]]
        return FunctionCall(alias, parsed_expression, tuple(columns_expr))
    elif (
        # Simple Clickhouse expression with no snuba syntax
        # ["ifNull(count(somthing), something)", None, None]
        isinstance(parsed_expression, (FunctionCall, CurriedFunctionCall))
        and not columns_expr
    ):
        return replace(parsed_expression, alias=alias)
    elif isinstance(parsed_expression, FunctionCall) and columns_expr:
        # Mix of clickhouse syntax and snuba syntax that generates a CurriedFunction
        # ["f(a)", "b", None]
        return CurriedFunctionCall(alias, parsed_expression, tuple(columns_expr),)
    else:
        raise ValueError(f"Invalid aggregation format {aggregation_function} {column}")
