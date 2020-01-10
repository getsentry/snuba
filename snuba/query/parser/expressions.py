import re

from typing import Any, Iterable, Optional

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.parser.functions import parse_function_to_expr
from snuba.util import is_function, QUOTED_LITERAL_RE

# We still have some cases were people pass clickhouse expressions in the
# aggregate clause of the Snuba query since snuba does not reject them.
# In order to process the aggregate first we need to turn those expressions
# into Snuba functions.
# This regex provides some very basic support to decompose those expressions.
AGGR_CLICKHOUSE_FUNCTION_RE = re.compile(
    r"^(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*\((?P<params>[\w\,\.\s]*)\)$"
)


def parse_expression(val: Any) -> Expression:
    if is_function(val, 0):
        return parse_function_to_expr(val)
    # TODO: This will use the schema of the dataset to decide
    # if the expression is a column or a literal.
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


def parse_aggregation(
    aggregation_function: str, column: Any, alias: Optional[str]
) -> Expression:
    m = AGGR_CLICKHOUSE_FUNCTION_RE.match(aggregation_function)
    if m:
        func_name = m.group("name")
        param_string = m.group("params") or ""
        param_list = [
            # We are not going to try to make complex parsing of what someone passes
            # where they should not. These are literals. And they will be escaped accordingly
            # IF someone passes a column name or another function, it won't work.
            parse_expression(p.strip())
            for p in param_string.split(",")
            if p.strip()
        ]
    else:
        func_name = aggregation_function
        param_list = []

    if not isinstance(column, (list, tuple)):
        columns: Iterable[Any] = (column,)
    else:
        columns = column

    columns_expr = [parse_expression(column) for column in columns if column]

    if param_list and columns_expr:
        # This is a curried function
        return CurriedFunctionCall(
            alias,
            FunctionCall(None, func_name, tuple(param_list)),
            tuple(columns_expr),
        )
    else:
        return FunctionCall(
            alias, func_name, tuple(columns_expr) if columns_expr else tuple(param_list)
        )
