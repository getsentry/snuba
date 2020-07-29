import re
from dataclasses import replace
from enum import Enum
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.query.dsl import divide, minus, multiply, plus
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


class LowPriOperator(Enum):
    PLUS = "+"
    MINUS = "-"


class HighPriOperator(Enum):
    MULTIPLY = "*"
    DIVIDE = "/"


class LowPriTuple(NamedTuple):
    op: LowPriOperator
    arithm: Expression


class HighPriTuple(NamedTuple):
    op: HighPriOperator
    arithm: Expression


HighPriArithmetic = Union[Node, HighPriTuple, Sequence[HighPriTuple]]
LowPriArithmetic = Union[Node, LowPriTuple, Sequence[LowPriTuple]]


def get_arithmetic_function(
    operator: Enum,
) -> Callable[[Expression, Expression, Optional[str]], FunctionCall]:
    return {
        HighPriOperator.MULTIPLY: multiply,
        HighPriOperator.DIVIDE: divide,
        LowPriOperator.PLUS: plus,
        LowPriOperator.MINUS: minus,
    }[operator]


def get_arithmetic_expression(
    term: Expression, exp: Union[LowPriArithmetic, HighPriArithmetic],
) -> Expression:
    if isinstance(exp, Node):
        return term
    if isinstance(exp, (LowPriTuple, HighPriTuple)):
        return get_arithmetic_function(exp.op)(term, exp.arithm, None)
    elif isinstance(exp, list):
        for elem in exp:
            term = get_arithmetic_function(elem.op)(term, elem.arithm, None)
    return term


class ClickhouseVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return str(node.text)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return Column(None, None, node.text)

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Expression]
    ) -> LowPriTuple:
        left, right = visited_children

        return LowPriTuple(op=left, arithm=right)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Expression]
    ) -> HighPriTuple:
        left, right = visited_children

        return HighPriTuple(op=left, arithm=right)

    def visit_low_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> LowPriOperator:

        return LowPriOperator(node.text)

    def visit_high_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> HighPriOperator:

        return HighPriOperator(node.text)

    def visit_arithmetic_term(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        _, term, _ = visited_children

        return term

    def visit_low_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, LowPriArithmetic],
    ) -> Expression:
        _, term, _, exp = visited_children

        return get_arithmetic_expression(term, exp)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, HighPriArithmetic],
    ) -> Expression:
        _, term, _, exp = visited_children

        return get_arithmetic_expression(term, exp)

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
    raise ParsingException(
        f"Expression to parse can only be a function or a string: {val}"
    )


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

    if not isinstance(column, (list, tuple)):
        columns: Iterable[Any] = (column,)
    else:
        columns = column

    columns_expr = [parse_expression(column) for column in columns if column]

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
