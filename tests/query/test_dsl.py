from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import CurriedFunctionCall, FunctionCall, Literal


def test_function_syntax() -> None:
    assert f.equals(1, 1, alias="eq") == FunctionCall(
        "eq", "equals", parameters=(Literal(None, 1), Literal(None, 1))
    )


def test_curried_function() -> None:
    assert cf.quantile(0.9)(column("measurement"), alias="p90") == CurriedFunctionCall(
        alias="p90",
        internal_function=f.quantile(0.9),
        parameters=(column("measurement"),),
    )
