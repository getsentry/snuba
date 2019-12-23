from typing import Any

from snuba.query.expressions import Column, Expression, Literal
from snuba.query.parser.functions import parse_function_to_expr
from snuba.util import is_function, QUOTED_LITERAL_RE


def parse_expression(val: Any) -> Expression:
    if is_function(val, 0):
        return parse_function_to_expr(val)
    # TODO: This will use the schema of the dataset to decide
    # if the expression is a column or a literal.
    if QUOTED_LITERAL_RE.match(val):
        return Literal(None, val[1:-1])
    else:
        return Column(None, val, None)
