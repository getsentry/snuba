from snuba.query.expressions import Column, Expression, FunctionCall


def test_iterate() -> None:
    column1 = Column("a2", "c1", "t1")
    column2 = Column("a2", "c2", "t1")
    function_1 = FunctionCall("af", "f1", [column1, column2])
    column3 = Column("a2", "c2", "t1")
    function_2 = FunctionCall("af", "f2", [column3, function_1])

    expected = [function_2, column3, function_1, column1, column2]
    assert list(function_2) == expected


def test_mapping_column_list() -> None:
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
    f = FunctionCall("a3", "f", [])
    f3 = FunctionCall("a4", "f", [f])

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, FunctionCall) and e.get_function_name() == "f":
            return f3
        return e

    c2 = Column("a2", "c2", "t1")
    f2 = FunctionCall("a4", "f", [c2, f])

    # Only the external function is going to be replaced since, when map returns a new
    # column, we expect the closure to have takern care of its own children.
    f2.map(replace_expr)
    iterate = list(f2)
    expected = [f2, c2, f3, f]

    assert iterate == expected
