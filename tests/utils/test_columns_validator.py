from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pytest

from snuba.utils.schemas import (
    UUID,
    Column,
    ColumnSet,
    ColumnValidator,
    Date,
    Float,
    Int,
    InvalidColumnType,
    String,
    Tuple,
    UInt,
)

COLUMNS = ColumnSet(
    [
        Column("str_param", String()),
        Column("uint_param", UInt(32)),
        Column("int_param", Int(32)),
        Column("float_param", Float(64)),
        Column("uuid_param", UUID()),
        Column("date_param", Date()),
        Column(
            "tuple_param",
            Tuple(
                (
                    UUID(),
                    Int(32),
                )
            ),
        ),
    ]
)


@pytest.mark.parametrize(
    "column_name, values, is_valid",
    [
        # valid column types
        pytest.param("uint_param", [1, 2, 3, 4], True),
        pytest.param("str_param", ["hi", "hello"], True),
        pytest.param("int_param", [1, 2, 3, 4], True),
        pytest.param("float_param", [1.20, 2.0], True),
        pytest.param("uuid_param", ["06a910bc-7682-4c76-b818-666124cc8898"], True),
        pytest.param(
            "tuple_param",
            [
                (
                    "06a910bc-7682-4c76-b818-666124cc8898",
                    1,
                )
            ],
            True,
        ),
        # invalid column types
        pytest.param("uint_param", [-17], False),
        pytest.param("str_param", ["hi", 3], False),
        pytest.param("int_param", [1, 2, 3, 4.0], False),
        pytest.param("float_param", [1.20, 2], False),
        pytest.param("uuid_param", ["123456"], False),
        pytest.param(
            "tuple_param",
            [
                (
                    "06a910bc-7682-4c76-b818-666124cc8898",
                    "this_should_be_int",
                )
            ],
            False,
        ),
        # wrong length
        pytest.param("tuple_param", [("06a910bc-7682-4c76-b818-666124cc8898",)], False),
        # unsupported column types
        pytest.param("date_param", [datetime.now()], False),
    ],
)
def test_validator(column_name: str, values: Sequence[Any], is_valid: bool) -> None:
    col_validator = ColumnValidator(COLUMNS)

    if is_valid:
        col_validator.validate(column_name, values)
    else:
        # invalid inputs raise InvalidColumnType, except tuple-arity mismatches
        # which trip an assert in _valid_tuple
        with pytest.raises((InvalidColumnType, AssertionError)):
            col_validator.validate(column_name, values)
