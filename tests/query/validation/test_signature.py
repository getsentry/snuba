from typing import Sequence

import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, String
from snuba.query.expressions import Column as ColumnExpr, Expression, Literal
from snuba.query.validation import InvalidFunctionCallException
from snuba.query.validation.signature import (
    Any,
    Column,
    ParamType,
    SignatureValidator,
)

test_cases = [
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [Any(), Any()],
        False,
        False,
        id="Valid Expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [Column({String}), Any()],
        False,
        False,
        id="Valid Specific Expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="timestamp"),
            Literal(None, "param"),
        ),
        [Column({String}), Any()],
        False,
        True,
        id="Invalid specific expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [Column({String})],
        True,
        False,
        id="Valid expression with optional parameters",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            Literal(None, "param"),
        ),
        [Column({String}, nullable=False), Any()],
        False,
        True,
        id="Invalid, Non nullable expression required",
    ),
]


@pytest.mark.parametrize(
    "expressions, expected_types, extra_param, should_raise", test_cases
)
def test_like_validator(
    expressions: Sequence[Expression],
    expected_types: Sequence[ParamType],
    extra_param: bool,
    should_raise: bool,
) -> None:
    schema = ColumnSet(
        [
            ("event_id", String()),
            ("level", Nullable(String())),
            ("str_col", String()),
            ("timestamp", DateTime()),
            ("received", Nullable(DateTime())),
        ]
    )

    validator = SignatureValidator(expected_types, extra_param)

    if should_raise:
        with pytest.raises(InvalidFunctionCallException):
            validator.validate(expressions, schema)
    else:
        validator.validate(expressions, schema)
