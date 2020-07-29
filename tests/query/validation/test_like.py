import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, String
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.parser.exceptions import ValidationException
from snuba.query.validation.like import LikeFunctionValidator

test_cases = [
    pytest.param(
        FunctionCall(
            None,
            "f",
            (
                Column(alias=None, table_name=None, column_name="event_id"),
                Literal(None, "param"),
            ),
        ),
        False,
        id="Not a like expression",
    ),
    pytest.param(
        FunctionCall(
            None,
            "like",
            (
                Column(alias=None, table_name=None, column_name="str_col"),
                Literal(None, "asd"),
            ),
        ),
        False,
        id="Valid like over a string",
    ),
    pytest.param(
        FunctionCall(
            None,
            "like",
            (
                Column(alias=None, table_name=None, column_name="timestamp"),
                Literal(None, "asd"),
            ),
        ),
        True,
        id="Invalid like over a datetime",
    ),
    pytest.param(
        FunctionCall(
            None,
            "like",
            (
                Column(alias=None, table_name=None, column_name="received"),
                Literal(None, "asd"),
            ),
        ),
        True,
        id="Invalid like over a datetime",
    ),
]


@pytest.mark.parametrize("expression, should_rise", test_cases)
def test_like_validator(expression: Expression, should_rise: bool) -> None:
    schema = ColumnSet(
        [
            ("event_id", String()),
            ("str_col", String()),
            ("timestamp", DateTime()),
            ("received", Nullable(DateTime())),
        ]
    )

    validator = LikeFunctionValidator()

    if should_rise:
        with pytest.raises(ValidationException):
            validator.validate(expression, schema)
    else:
        validator.validate(expression, schema)
