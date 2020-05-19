from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import binary_condition, ConditionFunctions
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import OrderBy, OrderByDirection, Query


def test_iterate_over_query():
    """
    Creates a query with the new AST and iterate over all expressions.
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(
        None, ConditionFunctions.EQ, column1, Literal(None, "1")
    )

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[function_1],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        order_by=[orderby],
    )

    expected_expressions = [
        # selected columns
        column1,
        column2,
        function_1,
        # condition
        column1,
        Literal(None, "1"),
        condition,
        # groupby
        column1,
        column2,
        function_1,
        # order by
        column2,
        function_2,
    ]

    assert list(query.get_all_expressions()) == expected_expressions


def test_replace_expression():
    """
    Create a query with the new AST and replaces a function with a different function
    replaces f1(...) with tag(f1)
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(
        None, ConditionFunctions.EQ, function_1, Literal(None, "1")
    )

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[function_1],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        order_by=[orderby],
    )

    def replace(exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "f1":
            return FunctionCall(exp.alias, "tag", (Literal(None, "f1"),))
        return exp

    query.transform_expressions(replace)

    expected_query = Query(
        {},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[FunctionCall("alias", "tag", (Literal(None, "f1"),))],
        array_join=None,
        condition=binary_condition(
            None,
            ConditionFunctions.EQ,
            FunctionCall("alias", "tag", (Literal(None, "f1"),)),
            Literal(None, "1"),
        ),
        groupby=[FunctionCall("alias", "tag", (Literal(None, "f1"),))],
        having=None,
        order_by=[orderby],
    )

    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()

    assert list(query.get_all_expressions()) == list(
        expected_query.get_all_expressions()
    )
