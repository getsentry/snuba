from snuba.query.expressions import Column, Expression


def get_columns_in_expression(exp: Expression) -> set[Column]:
    """
    Returns all the columns referenced in an arbitrary AST expression.
    """
    return {e for e in exp if isinstance(e, Column)}
