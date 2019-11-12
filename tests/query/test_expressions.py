from snuba.query.expressions import (
    Aggregation,
    Column,
    Expression,
    FunctionCall
)


def test_iterate() -> None:
    """
    Test iteration over a subtree. The subtree is a function call in the form
    f2(c3, f1(c1, c2))
    """
    column1 = Column(None, "c1", "t1")
    column2 = Column(None, "c2", "t1")
    function_1 = FunctionCall(None, "f1", [column1, column2])
    column3 = Column(None, "c2", "t1")
    function_2 = FunctionCall(None, "f2", [column3, function_1])

    expected = [function_2, column3, function_1, column1, column2]
    assert list(function_2) == expected


def test_aliased_cols() -> None:
    """
    Test iteration whan columns have aliases
    """
    column1 = Column(None, "c1", "t1")
    column2 = Column("a2", "c2", "t1")
    function_1 = FunctionCall(None, "f1", [column1, column2])
    column3 = Column(None, "c2", "t1")
    function_2 = FunctionCall("af1", "f2", [column3, function_1])

    expected = [function_2, column3, function_1, column1, column2]
    assert list(function_2) == expected


def test_mapping_column_list() -> None:
    """
    Perform a simple mapping over a series of expressions.
    """

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c2":
            return FunctionCall(None, "f", [e])
        return e

    column1 = Column(None, "c1", "t1")
    column2 = Column(None, "c2", "t2")
    column3 = Column(None, "c3", "t3")
    selected_cols = [column1, column2, column3]
    new_selected_cols = list(map(
        replace_col,
        selected_cols
    ))

    assert new_selected_cols[0] == column1
    assert new_selected_cols[2] == column3
    f = new_selected_cols[1]
    assert isinstance(f, FunctionCall)
    assert f.function_name == "f"
    assert f.parameters == [column2]


def test_add_alias() -> None:
    """
    Adds an alias to a column referenced in a function
    """
    column1 = Column(None, "c1", "t1")
    column2 = Column("a", "c1", "t1")

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c1":
            return column2
        return e
    f = FunctionCall(None, "f", [column1])

    f.transform(replace_expr)
    expected = [f, column2]
    assert list(f) == expected


def test_mapping_complex_expression() -> None:
    """
    Maps over an Expression container:
    f(c1, fB(f())) -> f(c1, fB(f(f())))
    """

    f5 = FunctionCall("a", "f", [])
    f4 = FunctionCall(None, "f", [f5])
    f3 = FunctionCall(None, "f", [])

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, FunctionCall) and e.function_name == "f":
            return f4
        return e

    c1 = Column(None, "c1", "t1")
    f2 = FunctionCall(None, "fB", [f3])
    f1 = FunctionCall(None, "f", [c1, f2])

    # Only the external function is going to be replaced since, when map returns a new
    # column, we expect the func to have takern care of its own children.
    f1.transform(replace_expr)
    iterate = list(f1)
    expected = [f1, c1, f2, f4, f5]

    assert iterate == expected


def test_aggregations() -> None:
    column1 = Column(None, "c1", "t1")
    column2 = Column(None, "c2", "t1")
    function_1 = FunctionCall(None, "f1", [column1, column2])
    column3 = Column(None, "c3", "t1")
    function_2 = FunctionCall(None, "f2", [column3, function_1])

    aggregation = Aggregation(None, "count", [function_2])
    expected = [function_2, column3, function_1, column1, column2]
    assert list(aggregation) == expected

    column4 = Column(None, "c4", "t2")
    aggregation.transform(
        lambda e: column4 if isinstance(e, Column) and e.column_name == "c1" else e
    )
    expected = [function_2, column3, function_1, column4, column2]
    assert list(aggregation) == expected
