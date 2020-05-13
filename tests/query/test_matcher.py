
from snuba.query.expressions import FunctionCall
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.matcher import Any, Param, Function, String

def test_base_expression() -> None:
    expression = FunctionCall(
        None,
        "f",
        (
            FunctionCall(None, "f2", (ColumnExpr(None, "c", None), ColumnExpr(None, "c2", None))),
            ColumnExpr(None, "c3", None),
        )
    )

    matcher = Function(
        None,
        Param("f_name", Any()),
        (Param("inner", Function(None, Param("inner_name", String("f2")), None)), Any())
    )

    result, params = matcher.match(expression)

    assert result == True
    assert params == {
        "f_name": "f",
        "inner": FunctionCall(None, "f2", (ColumnExpr(None, "c", None), ColumnExpr(None, "c2", None))),
        "inner_name": "f2",
    }
