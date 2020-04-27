import numbers
from datetime import date, datetime
from typing import Any, Callable, List, Optional, TypeVar, Tuple, Union

from snuba.query.expressions import Expression, Literal, FunctionCall
from snuba.query.parser.strings import parse_string
from snuba.util import is_function

TExpression = TypeVar("TExpression")


def parse_function(
    output_builder: Callable[[Optional[str], str, List[TExpression]], TExpression],
    simple_expression_builder: Callable[[str], TExpression],
    literal_builder: Callable[
        [Optional[Union[str, datetime, date, List[Any], Tuple[Any], numbers.Number]]],
        TExpression,
    ],
    expr: Any,
    depth: int = 0,
) -> TExpression:
    """
    Parses a function expression in the Snuba syntax and produces the expected data structure
    to be used in the Query object.

    It relies on three functions:
    - output_builder, this puts alias, function name and parameters together
    - simple_expression_builder, processes one column given the string name
    - literal_builder, processes any individual type that represent a literal.

    The goal of having these three functions is to preserve the parsing algorithm
    but being able to either produce an AST or the old Clickhouse syntax.
    """
    function_tuple = is_function(expr, depth)
    if function_tuple is None:
        raise ValueError(
            "complex_column_expr was given an expr %s that is not a function at depth %d."
            % (expr, depth)
        )

    name, args, alias = function_tuple
    out: List[TExpression] = []
    i = 0
    while i < len(args):
        next_2 = args[i : i + 2]
        if is_function(next_2, depth + 1):
            out.append(
                parse_function(
                    output_builder,
                    simple_expression_builder,
                    literal_builder,
                    next_2,
                    depth + 1,
                )
            )
            i += 2
        else:
            nxt = args[i]
            if is_function(nxt, depth + 1):  # Embedded function
                out.append(
                    parse_function(
                        output_builder,
                        simple_expression_builder,
                        literal_builder,
                        nxt,
                        depth + 1,
                    )
                )
            elif isinstance(nxt, str):
                out.append(simple_expression_builder(nxt))
            else:
                out.append(literal_builder(nxt))
            i += 1

    return output_builder(alias, name, out)


def parse_function_to_expr(expr: Any) -> Expression:
    """
    A rudimentary parser for functions implemented to test
    the proper functioning of the function above.
    The real parser will be in a different data structure.
    """

    def literal_builder(
        val: Optional[Union[str, datetime, date, List[Any], Tuple[Any], numbers.Number]]
    ) -> Expression:
        assert val is None or isinstance(val, (bool, str, float, int))
        return Literal(None, val)

    def output_builder(
        alias: Optional[str], name: str, params: List[Expression]
    ) -> Expression:
        return FunctionCall(alias, name, tuple(params))

    return parse_function(output_builder, parse_string, literal_builder, expr, 0,)
