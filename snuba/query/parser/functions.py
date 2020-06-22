import numbers
import re
from datetime import date, datetime
from typing import Any, Callable, List, Optional, Tuple, TypeVar, Union

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.clickhouse.escaping import escape_identifier
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.parser.strings import parse_string_to_expr
from snuba.state import get_config
from snuba.util import is_function

TExpression = TypeVar("TExpression")

TOPK_FUNCTION_RE = re.compile(r"^top([1-9]\d*)$")
APDEX_FUNCTION_RE = re.compile(r"^apdex\(\s*([^,]+)+\s*,\s*([\d]+)+\s*\)$")
IMPACT_FUNCTION_RE = re.compile(
    r"^impact\(\s*([^,]+)+\s*,\s*([\d]+)+\s*,\s*([^,]+)+\s*\)$"
)
FAILURE_RATE_FUNCTION_RE = re.compile(r"^failure_rate\(\)$")


def function_expr(fn: str, args_expr: str = "") -> str:
    """
    DEPRECATED. Please do not add anything else here. In order to manipulate the
    query, create a QueryProcessor and register it into your dataset.

    Generate an expression for a given function name and an already-evaluated
    args expression. This is a place to define convenience functions that evaluate
    to more complex expressions.

    """
    if fn.startswith("apdex("):
        match = APDEX_FUNCTION_RE.match(fn)
        if match:
            return "(countIf({col} <= {satisfied}) + (countIf(({col} > {satisfied}) AND ({col} <= {tolerated})) / 2)) / count()".format(
                col=escape_identifier(match.group(1)),
                satisfied=match.group(2),
                tolerated=int(match.group(2)) * 4,
            )
        raise ValueError("Invalid format for apdex()")
    elif fn.startswith("impact("):
        match = IMPACT_FUNCTION_RE.match(fn)
        if match:
            apdex = "(countIf({col} <= {satisfied}) + (countIf(({col} > {satisfied}) AND ({col} <= {tolerated})) / 2)) / count()".format(
                col=escape_identifier(match.group(1)),
                satisfied=match.group(2),
                tolerated=int(match.group(2)) * 4,
            )

            return "(1 - {apdex}) + ((1 - (1 / sqrt(uniq({user_col})))) * 3)".format(
                apdex=apdex, user_col=escape_identifier(match.group(3)),
            )
        raise ValueError("Invalid format for impact()")
    elif fn.startswith("failure_rate("):
        match = FAILURE_RATE_FUNCTION_RE.match(fn)
        if match:
            return "countIf(notIn(transaction_status, tuple({ok}, {cancelled}, {unknown}))) / count()".format(
                ok=SPAN_STATUS_NAME_TO_CODE["ok"],
                cancelled=SPAN_STATUS_NAME_TO_CODE["cancelled"],
                unknown=SPAN_STATUS_NAME_TO_CODE["unknown_error"],
            )
        raise ValueError("Invalid format for failure_rate()")
    # For functions with no args, (or static args) we allow them to already
    # include them as part of the function name, eg, "count()" or "sleep(1)"
    if not args_expr and fn.endswith(")"):
        return fn

    # Convenience topK function eg "top10", "top3" etc.
    topk = TOPK_FUNCTION_RE.match(fn)
    if topk:
        return "topK({})({})".format(topk.group(1), args_expr)

    # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where
    # a number was expected.
    if fn == "uniq":
        return "ifNull({}({}), 0)".format(fn, args_expr)

    # emptyIfNull(col) is a simple pseudo function supported by Snuba that expands
    # to the actual clickhouse function ifNull(col, '') Until we figure out the best
    # way to disambiguate column names from string literals in complex functions.
    if fn == "emptyIfNull" and args_expr:
        return "ifNull({}, '')".format(args_expr)

    # Workaround for https://github.com/ClickHouse/ClickHouse/issues/11622
    # Some distributed queries fail when arrays are passed as array(1,2,3)
    # and work when they are passed as [1, 2, 3]
    if get_config("format_clickhouse_arrays", 1) and fn == "array":
        return f"[{args_expr}]"

    # default: just return fn(args_expr)
    return "{}({})".format(fn, args_expr)


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
    Parses a function expression in the Snuba syntax and produces an AST Expression.
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

    return parse_function(
        output_builder, parse_string_to_expr, literal_builder, expr, 0,
    )
