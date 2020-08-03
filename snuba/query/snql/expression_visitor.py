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


def exp_visit_function_name(left, node: Node, visited_children: Iterable[Any]) -> str:
    return str(node.text)


def exp_visit_column_name(left, node: Node, visited_children: Iterable[Any]) -> Column:
    return Column(None, None, node.text)


def exp_visit_low_pri_tuple(
    left, node: Node, visited_children: Tuple[LowPriOperator, Expression]
) -> LowPriTuple:
    left, right = visited_children

    return LowPriTuple(op=left, arithm=right)


def exp_visit_high_pri_tuple(
    left, node: Node, visited_children: Tuple[HighPriOperator, Expression]
) -> HighPriTuple:
    left, right = visited_children

    return HighPriTuple(op=left, arithm=right)


def exp_visit_low_pri_op(
    left, node: Node, visited_children: Iterable[Any]
) -> LowPriOperator:

    return LowPriOperator(node.text)


def exp_visit_high_pri_op(
    left, node: Node, visited_children: Iterable[Any]
) -> HighPriOperator:

    return HighPriOperator(node.text)


def exp_visit_arithmetic_term(
    left, node: Node, visited_children: Tuple[Any, Expression, Any]
) -> Expression:
    _, term, _ = visited_children

    return term


def exp_visit_low_pri_arithmetic(
    left, node: Node, visited_children: Tuple[Any, Expression, Any, LowPriArithmetic],
) -> Expression:
    _, term, _, exp = visited_children

    return get_arithmetic_expression(term, exp)


def exp_visit_high_pri_arithmetic(
    left, node: Node, visited_children: Tuple[Any, Expression, Any, HighPriArithmetic],
) -> Expression:
    _, term, _, exp = visited_children

    return get_arithmetic_expression(term, exp)


def exp_visit_numeric_literal(
    left, node: Node, visited_children: Iterable[Any]
) -> Literal:
    try:
        return Literal(None, int(node.text))
    except Exception:
        return Literal(None, float(node.text))


def exp_visit_quoted_literal(
    left, node: Node, visited_children: Tuple[Any, Node, Any]
) -> Literal:
    _, val, _ = visited_children
    return Literal(None, val.text)


def exp_visit_parameter(
    left, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
) -> Expression:
    param, _, _, _, = visited_children
    return param


def exp_visit_parameters_list(
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


def exp_visit_function_call(
    left,
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


def exp_generic_visit(left, node: Node, visited_children: Any) -> Any:
    if isinstance(visited_children, list) and len(visited_children) == 1:
        return visited_children[0]
    return visited_children or node
