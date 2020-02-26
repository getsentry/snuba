from snuba.datasets.tags_column_processor import NESTED_COL_EXPR_RE
from snuba.query.expressions import Column, Expression, MappingColumn, Literal
from snuba.util import QUOTED_LITERAL_RE


def parse_string(val: str) -> Expression:
    if isinstance(val, str):
        match = NESTED_COL_EXPR_RE.match(val)
        if match:
            col_name = match[1]
            key_name = match[2]
            return MappingColumn(
                # We pass the original expression as alias so that the query processors
                # know what the client expects as column name.
                # TODO: apply alias on all AST nodes created during parsing to make the original
                # name visible to the query processors.
                alias=val,
                column_name=col_name,
                table_name=None,
                key=key_name,
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
