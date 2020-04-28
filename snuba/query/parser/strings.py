from snuba.datasets.tags_column_processor import NESTED_COL_EXPR_RE
from snuba.query.expressions import Column, Expression, Literal, SubscriptableReference
from snuba.util import QUOTED_LITERAL_RE


def parse_string_to_expr(val: str) -> Expression:
    match = NESTED_COL_EXPR_RE.match(val)
    if match:
        col_name = match[1]
        key_name = match[2]
        return SubscriptableReference(
            # TODO: apply alias on all AST nodes created during parsing to make the original
            # column name (that the client expects) available to the translator.
            alias=val,
            referenced_column=Column(None, col_name, None),
            key=Literal(None, key_name),
        )

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
