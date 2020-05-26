import re

from snuba.query.expressions import (
    Column,
    Expression,
    Literal,
    SubscriptableReference,
)
from snuba.util import QUOTED_LITERAL_RE

# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


def parse_string_to_expr(val: str) -> Expression:
    """
    Parses a string expression found in a Snuba Query into an AST expression.
    This is supposed to work for any non structured expression in the Snuba Query.
    """
    match = NESTED_COL_EXPR_RE.match(val)
    if match:
        col_name = match[1]
        key_name = match[2]
        return SubscriptableReference(
            # We set the alias of this expression with the original value found in the Query
            # (tags[asd]) so that it will be preserved across the query processing pipeline
            # and it will be applied to the Clickhouse query as alias. As a consequence,
            # no matter what expression this value is transformed into, the client will receive
            # the value they provided in the response (as alias).
            #
            # TODO: adopt this approach to all expression generated during parsing.
            alias=val,
            column=Column(None, col_name, None),
            key=Literal(None, key_name),
        )

    # TODO: This will use the schema of the dataset/entity to decide if the expression is
    # a column or a literal.
    if val.isdigit():
        return Literal(None, int(val))
    else:
        try:
            return Literal(None, float(val))
        except Exception:
            if QUOTED_LITERAL_RE.match(val):
                return Literal(None, val[1:-1])
            else:
                return Column(None, val, None)
