from snuba.query.expressions import (
    Column,
    Expression,
    Literal,
)
from snuba.util import QUOTED_LITERAL_RE, parse_datetime


def parse_string_to_expr(val: str) -> Expression:
    """
    Parses a string expression found in a Snuba Query into an AST
    expression. This is supposed to work for any non structured expression
    in the Snuba Query.
    """
    # TODO: This will use the schema of the dataset/entity to decide if the expression is
    # a column or a literal. In the meantime, make some guesses about the value.

    if QUOTED_LITERAL_RE.match(val):
        return Literal(None, val[1:-1])

    if val.isdigit():
        return Literal(None, int(val))

    try:
        return Literal(None, float(val))
    except Exception:
        pass

    try:
        return Literal(None, parse_datetime(val))
    except ValueError:
        pass

    return Column(None, None, val)
