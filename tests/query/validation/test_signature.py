from typing import Sequence

import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, String
from snuba.query.expressions import (
    Column as ColumnExpr,
    Expression,
    Literal as LiteralExpr,
)
from snuba.query.validation import InvalidFunctionCall
from snuba.query.validation.signature import (
    Any,
    Column,
    Literal,
    ParamType,
    SignatureValidator,
)

test_cases = [
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            LiteralExpr(None, "param"),
        ),
        [Any(), Any()],
        False,
        False,
        id="Valid Expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            LiteralExpr(None, "param"),
        ),
        [Column({String}), Any()],
        False,
        False,
        id="Valid Specific Expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="timestamp"),
            LiteralExpr(None, "param"),
        ),
        [Column({String}), Any()],
        False,
        True,
        id="Invalid specific expression",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="event_id"),
            LiteralExpr(None, "param"),
        ),
        [Column({String})],
        True,
        False,
        id="Valid expression with optional parameters",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            LiteralExpr(None, "param"),
        ),
        [Column({String}, allow_nullable=False), Any()],
        False,
        True,
        id="Invalid, Non nullable expression required",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            LiteralExpr(None, "param"),
        ),
        [Column({String}), Literal({str})],
        False,
        False,
        id="Valid Literal type",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            LiteralExpr(None, "param"),
        ),
        [Column({String}), Literal({float, int})],
        False,
        True,
        id="Invalid Literal type",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            LiteralExpr(None, None),
        ),
        [Column({String}), Literal({float, int}, allow_nullable=True)],
        False,
        False,
        id="None value valid when allow_nullable is True",
    ),
    pytest.param(
        (
            ColumnExpr(alias=None, table_name=None, column_name="level"),
            LiteralExpr(None, None),
        ),
        [Column({String}), Literal({float, int})],
        False,
        True,
        id="None value invalid by default",
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
            ("level", String([Nullable()])),
            ("str_col", String()),
            ("timestamp", DateTime()),
            ("received", DateTime([Nullable()])),
        ]
    )

    validator = SignatureValidator(expected_types, extra_param)

    if should_raise:
        with pytest.raises(InvalidFunctionCall):
            validator.validate(expressions, schema)
    else:
        validator.validate(expressions, schema)
