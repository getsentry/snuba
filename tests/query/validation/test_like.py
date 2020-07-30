from typing import Sequence

import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, String
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.parser.exceptions import ValidationException
from snuba.query.validation.like import (
    AnyType,
    ColumnType,
    ParamType,
    SignatureValidator,
)

test_cases = [
    pytest.param(
        (
            Column(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [AnyType(), AnyType()],
        False,
        False,
        id="Valid Expression",
    ),
    pytest.param(
        (
            Column(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [ColumnType({String}), AnyType()],
        False,
        False,
        id="Valid Specific Expression",
    ),
    pytest.param(
        (
            Column(alias=None, table_name=None, column_name="timestamp"),
            Literal(None, "param"),
        ),
        [ColumnType({String}), AnyType()],
        False,
        True,
        id="Invalid specific expression",
    ),
    pytest.param(
        (
            Column(alias=None, table_name=None, column_name="event_id"),
            Literal(None, "param"),
        ),
        [ColumnType({String})],
        True,
        False,
        id="Valid expression with optional parameters",
    ),
]


@pytest.mark.parametrize(
    "expressions, expected_types, mandatory, should_rise", test_cases
)
def test_like_validator(
    expressions: Sequence[Expression],
    expected_types: Sequence[ParamType],
    mandatory: bool,
    should_rise: bool,
) -> None:
    schema = ColumnSet(
        [
            ("event_id", String()),
            ("str_col", String()),
            ("timestamp", DateTime()),
            ("received", Nullable(DateTime())),
        ]
    )

    validator = SignatureValidator(expected_types, mandatory)

    if should_rise:
        with pytest.raises(ValidationException):
            validator.validate(expressions, schema)
    else:
        validator.validate(expressions, schema)
