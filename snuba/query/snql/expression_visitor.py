import re
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

from parsimonious.nodes import Node

from snuba.query.dsl import divide, minus, multiply, plus
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
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


ARITHMETIC_OP_TO_FUNCTION = {
    HighPriOperator.MULTIPLY: multiply,
    HighPriOperator.DIVIDE: divide,
    LowPriOperator.PLUS: plus,
    LowPriOperator.MINUS: minus,
}


def get_arithmetic_function(
    operator: Enum,
) -> Callable[[Expression, Expression, Optional[str]], FunctionCall]:
    return ARITHMETIC_OP_TO_FUNCTION[operator]


def get_arithmetic_expression(
    term: Expression, exp: Union[LowPriArithmetic, HighPriArithmetic, Sequence[Any]],
) -> Expression:
    if isinstance(exp, Node):
        return term
    if isinstance(exp, (LowPriTuple, HighPriTuple)):
        return get_arithmetic_function(exp.op)(term, exp.arithm, None)
    elif isinstance(exp, list):
        for elem in exp:
            if isinstance(elem, (LowPriTuple, HighPriTuple)):
                term = get_arithmetic_function(elem.op)(term, elem.arithm, None)
            elif isinstance(elem, list):
                _, op_tuple = elem
                term = get_arithmetic_function(op_tuple.op)(term, op_tuple.arithm, None)
    return term


def visit_function_name(node: Node, visited_children: Iterable[Any]) -> str:
    return str(node.text)


def visit_column_name(node: Node, visited_children: Iterable[Any]) -> Column:
    return Column(None, None, node.text)


def visit_low_pri_tuple(
    node: Node, visited_children: Tuple[LowPriOperator, Any, Expression]
) -> LowPriTuple:
    left, _, right = visited_children
    return LowPriTuple(op=left, arithm=right)


def visit_high_pri_tuple(
    node: Node, visited_children: Tuple[HighPriOperator, Any, Expression]
) -> HighPriTuple:
    left, _, right = visited_children
    return HighPriTuple(op=left, arithm=right)


def visit_low_pri_op(node: Node, visited_children: Iterable[Any]) -> LowPriOperator:
    return LowPriOperator(node.text)


def visit_high_pri_op(node: Node, visited_children: Iterable[Any]) -> HighPriOperator:
    return HighPriOperator(node.text)


def visit_arithmetic_term(
    node: Node, visited_children: Tuple[Any, Expression]
) -> Expression:
    _, term = visited_children
    return term


def visit_low_pri_arithmetic(
    node: Node, visited_children: Tuple[Any, Expression, LowPriArithmetic],
) -> Expression:
    _, term, exp = visited_children
    return get_arithmetic_expression(term, exp)


def visit_high_pri_arithmetic(
    node: Node, visited_children: Tuple[Any, Expression, HighPriArithmetic],
) -> Expression:
    _, term, exp = visited_children

    return get_arithmetic_expression(term, exp)


def visit_numeric_literal(node: Node, visited_children: Iterable[Any]) -> Literal:
    try:
        return Literal(None, int(node.text))
    except Exception:
        return Literal(None, float(node.text))


newline_re = re.compile("((?:\\{2})*)(\\n)")


def visit_quoted_literal(node: Node, visited_children: Tuple[Any]) -> Literal:
    text = node.text[1:-1]
    text = newline_re.sub(text, "\n")
    match = text.replace("\\'", "'")
    return Literal(None, match)


def visit_parameter(
    node: Node, visited_children: Tuple[Expression, Any, Any, Any]
) -> Expression:
    param, _, _, _ = visited_children
    return param


def visit_parameters_list(
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


def generic_visit(node: Node, visited_children: Any) -> Any:
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
