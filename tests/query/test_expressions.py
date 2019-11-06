from snuba.query.expressions import Aggregation, Column, Expression, FunctionCall


def test_iterate() -> None:
    """
    Test iteration over a subtree. The subtree is a function call in the form
    f2(c3, f1(c1, c2))
    """
    column1 = Column("a2", "c1", "t1")
    column2 = Column("a2", "c2", "t1")
    function_1 = FunctionCall("af", "f1", [column1, column2])
    column3 = Column("a2", "c2", "t1")
    function_2 = FunctionCall("af", "f2", [column3, function_1])

    expected = [function_2, column3, function_1, column1, column2]
    assert list(function_2) == expected


def test_mapping_column_list() -> None:
    """
    Perform a simple mapping over a series of expressions.
    """

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.get_column_name() == "c2":
            return FunctionCall("a1", "f", [e])
        return e

    column1 = Column("a1", "c1", "t1")
    column2 = Column("a2", "c2", "t2")
    column3 = Column("a3", "c3", "t3")
    selected_cols = [column1, column2, column3]
    new_selected_cols = list(map(
        replace_col,
        selected_cols
    ))

    assert new_selected_cols[0] == column1
    assert new_selected_cols[2] == column3
    f = new_selected_cols[1]
    assert isinstance(f, FunctionCall)
    assert f.get_function_name() == "f"
    assert f.get_parameters() == [column2]


def test_mapping_complex_expression() -> None:
    """
    Maps over an Expression container:
    f(c1, fB(f())) -> f(c1, fB(f(f())))
    """

    f5 = FunctionCall("a5", "f", [])
    f4 = FunctionCall("a4", "f", [f5])
    f3 = FunctionCall("a3", "f", [])

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, FunctionCall) and e.get_function_name() == "f":
            return f4
        return e

    c1 = Column("c1", "c1", "t1")
    f2 = FunctionCall("a2", "fB", [f3])
    f1 = FunctionCall("a1", "f", [c1, f2])

    # Only the external function is going to be replaced since, when map returns a new
    # column, we expect the closure to have takern care of its own children.
    f1.map(replace_expr)
    iterate = list(f1)
    expected = [f1, c1, f2, f4, f5]

    assert iterate == expected


def test_aggregations() -> None:
    column1 = Column("a2", "c1", "t1")
    column2 = Column("a2", "c2", "t1")
    function_1 = FunctionCall("af", "f1", [column1, column2])
    column3 = Column("a2", "c3", "t1")
    function_2 = FunctionCall("af", "f2", [column3, function_1])

    aggregation = Aggregation(None, "count", [function_2])
    expected = [function_2, column3, function_1, column1, column2]
    assert list(aggregation) == expected

    column4 = Column("a4", "c4", "t2")
    aggregation.map(
        lambda e: column4 if isinstance(e, Column) and e.get_column_name() == "c1" else e
    )
    expected = [function_2, column3, function_1, column4, column2]
    assert list(aggregation) == expected
