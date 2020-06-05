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
    Parses a string expression found in a Snuba Query into an AST
    expression. This is supposed to work for any non structured expression
    in the Snuba Query.

    This guarantees that string expressions that correspond to columns
    the user expect to see in the response (columns and tags[] expressions)
    have an alias in the AST. This alias corresponds to the string the
    user put into the query (which is the name of the column they will
    find in the result).
    This is done so that the column name provided by the user is preserved
    across the query processing pipeline and it is applied to the Clickhouse
    query as alias.

    As a consequence, the only requirement, when translating or transforming
    an expression is to preserve the alias of the input expression regardless
    to how it was generated, and by whom.
    """
    match = NESTED_COL_EXPR_RE.match(val)
    if match:
        col_name = match[1]
        key_name = match[2]
        return SubscriptableReference(
            alias=val, column=Column(None, None, col_name), key=Literal(None, key_name),
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
                return Column(val, None, val)
