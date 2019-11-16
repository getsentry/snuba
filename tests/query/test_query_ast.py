from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import binary_condition, ConditionFunctions
from snuba.query.expressions import (
    Aggregation,
    Column,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.query import OrderBy, OrderByDirection, Query


def test_iterate_over_query():
    """
    Creates a query with the new AST and iterate over all expressions.
    """
    column1 = Column(None, "c1", "t1")
    column2 = Column(None, "c2", "t1")
    function_1 = FunctionCall("alias", "f1", [column1, column2])

    agg1 = Aggregation("cnt", "count", [column1])
    condition = binary_condition(None, ConditionFunctions.EQ, function_1, Literal(None, "1"))

    orderby = OrderBy(OrderByDirection.ASC, agg1)

    query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[function_1],
        aggregations=[agg1],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        order_by=[orderby],
    )

    expected_expressions = [
        # selected columns
        column1, column2, function_1,
        # aggregations
        column1, agg1,
        # condition
        column1, column2, function_1, Literal(None, "1"), condition,
        # groupby
        column1, column2, function_1,
        # order by
        column1, agg1,
    ]

    assert list(query.get_all_expressions()) == expected_expressions


def test_replace_expression():
    """
    Create a query with the new AST and replaces a function with a different function
    replaces f1(...) with tag(f1)
    """
    column1 = Column(None, "c1", "t1")
    column2 = Column(None, "c2", "t1")
    function_1 = FunctionCall("alias", "f1", [column1, column2])

    agg1 = Aggregation("cnt", "count", [column1])
    condition = binary_condition(None, ConditionFunctions.EQ, function_1, Literal(None, "1"))

    orderby = OrderBy(OrderByDirection.ASC, agg1)

    query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[function_1],
        aggregations=[agg1],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        order_by=[orderby],
    )

    def replace(exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "f1":
            return FunctionCall(exp.alias, "tag", [Literal(None, "f1")])
        return exp

    query.transform_expression(replace)

    expected_expressions = [
        # selected columns
        Literal(None, "f1"), FunctionCall("alias", "tag", [Literal(None, "f1")]),
        # aggregations
        column1, agg1,
        # condition
        Literal(None, "f1"), FunctionCall("alias", "tag", [Literal(None, "f1")]), Literal(None, "1"),
        binary_condition(None, ConditionFunctions.EQ, FunctionCall("alias", "tag", [Literal(None, "f1")]), Literal(None, "1")),
        # groupby
        Literal(None, "f1"), FunctionCall("alias", "tag", [Literal(None, "f1")]),
        # order by
        column1, agg1,
    ]

    assert list(query.get_all_expressions()) == expected_expressions
