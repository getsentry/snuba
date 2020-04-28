from snuba.datasets.tags_column_processor import NESTED_COL_EXPR_RE
from snuba.query.expressions import Column, Expression, Literal, SubscriptableReference
from snuba.util import QUOTED_LITERAL_RE


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
            # TODO: apply aliases on all AST nodes created during parsing and that do not
            # already have one. This makes the field names from the original query
            # (that the client expects in the response) available when building the
            # Clickhouse query without leaking the logical schema.
            alias=val,
            referenced_column=Column(None, col_name, None),
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
