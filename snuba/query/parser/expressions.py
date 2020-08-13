import re
from dataclasses import replace
from typing import Any, Iterable, List, Optional, Tuple, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.datasets.dataset import Dataset
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.parser.exceptions import ParsingException
from snuba.query.parser.functions import parse_function_to_expr
from snuba.query.parser.strings import parse_string_to_expr
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
from snuba.util import is_function

FUNCTION_NAME_REGEX = r"[a-zA-Z_][a-zA-Z0-9_]*"
FUNCTION_NAME_RE = re.compile(FUNCTION_NAME_REGEX)


minimal_clickhouse_grammar = Grammar(
    fr"""
# This root element is needed because of the ambiguity of the aggregation
# function field which can mean a clickhouse function expression or the simple
# name of a clickhouse function.

root_element          = low_pri_arithmetic
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
function_name         = ~r"{FUNCTION_NAME_REGEX}"
open_paren            = "("
close_paren           = ")"
space                 = " "
comma                 = ","
"""
)


class ClickhouseVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

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


def parse_expression(
    val: Any, dataset: Dataset, arrayjoin: Optional[str] = ""
) -> Expression:
    """
    Parse a simple or structured expression encoded in the Snuba query language
    into an AST Expression.
    """
    if is_function(val, 0):
        return parse_function_to_expr(val, dataset, arrayjoin)
    if isinstance(val, str):
        return parse_string_to_expr(val)
    raise ParsingException(
        f"Expression to parse can only be a function or a string: {val}"
    )


def parse_aggregation(
    aggregation_function: str, column: Any, alias: Optional[str], dataset: Dataset
) -> Expression:
    """
    Aggregations, unfortunately, support both Snuba syntax and a subset
    of Clickhouse syntax. In order to preserve this behavior and still build
    a meaningful AST when parsing the query, we need to do some parsing of
    the clickhouse expression. (not that we should support this, but it is
    used in production).
    """

    if not isinstance(column, (list, tuple)):
        columns: Iterable[Any] = (column,)
    else:
        columns = column

    columns_expr = [parse_expression(column, dataset) for column in columns if column]

    matched = FUNCTION_NAME_RE.fullmatch(aggregation_function)

    if matched is not None:
        return FunctionCall(alias, aggregation_function, tuple(columns_expr))

    try:
        expression_tree = minimal_clickhouse_grammar.parse(aggregation_function)
    except Exception as cause:
        raise ParsingException(
            f"Cannot parse aggregation {aggregation_function}", cause
        ) from cause

    parsed_expression = ClickhouseVisitor().visit(expression_tree)

    if (
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
        raise ParsingException(
            f"Invalid aggregation format {aggregation_function} {column}"
        )
