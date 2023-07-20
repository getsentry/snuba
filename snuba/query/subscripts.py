from snuba.query.expressions import SubscriptableReference

# Contains functions to derive the expanded column names in the ColumnSet
# that correspond to a subscriptable nested column like tags and contexts.
# Example: the tags nested column is actually represented as two columns
# tags.key and tags.value in ColumnSet objects.

SUBSCRIPT_KEY = "key"


def _subscript_col_name(expression: SubscriptableReference, col_name: str) -> str:
    table_name = expression.column.table_name
    table = f"{table_name}." if table_name is not None else ""
    return f"{table}{expression.column.column_name}.{col_name}"


def subscript_key_column_name(expression: SubscriptableReference) -> str:
    return _subscript_col_name(expression, SUBSCRIPT_KEY)
