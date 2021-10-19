from typing import Set

from snuba.query.expressions import Column, Expression


def get_columns_in_expression(exp: Expression) -> Set[Column]:
    """
    Returns all the columns referenced in an arbitrary AST expression.
    """
    return set(e for e in exp if isinstance(e, Column))
