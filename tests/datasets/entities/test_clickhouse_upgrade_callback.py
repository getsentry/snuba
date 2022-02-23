from datetime import datetime
from typing import MutableSequence

import pytest

from snuba.datasets.entities.clickhouse_upgrade import (
    SplitRow,
    SplitSchema,
    calculate_score,
    row_similarity,
    split_metadata,
    split_row,
)
from snuba.reader import Row
from snuba.web import QueryResult


def test_split_metadata() -> None:
    result = QueryResult(
        result={
            "meta": [
                {"name": "field1", "type": "String"},
                {"name": "field2", "type": "Datetime"},
                {"name": "field3", "type": "Float64"},
                {"name": "field4", "type": "Enum"},
            ],
            "data": [],
            "totals": {},
            "profile": None,
            "trace_output": "asd",
        },
        extra={"stats": {}, "sql": "select something", "experiments": {}},
    )

    split_schema = split_metadata(result)
    assert split_schema.complete is False
    assert split_schema.key_cols == ("field1", "field2")
    assert split_schema.value_cols == ("field3",)

    row = split_row(
        {
            "field1": "asd",
            "field2": datetime(2022, 1, 1, 0, 0, 0),
            "field3": 0.01,
            "field4": "asd",
        },
        split_schema,
    )

    assert row == SplitRow(key=("asd", datetime(2022, 1, 1, 0, 0, 0)), values=(0.01,))


TESTS = [
    pytest.param(
        SplitRow(key=("a", "b"), values=(1.0, 1.0)),
        SplitRow(key=("a",), values=(1.0, 1.0)),
        0.0,
        id="Icompatible schema",
    ),
    pytest.param(
        SplitRow(key=("a", "b"), values=(1.0, 1.0)),
        SplitRow(key=("a", "c"), values=(1.0, 1.0)),
        0.0,
        id="Icompatible schema keys",
    ),
    pytest.param(
        SplitRow(key=("a", "b"), values=(1.0, 1.0)),
        SplitRow(key=("a", "b"), values=(1.0, 1.0, 2.0)),
        0.0,
        id="Incompatible values",
    ),
    pytest.param(
        SplitRow(key=("a", "b"), values=tuple()),
        SplitRow(key=("a", "b"), values=tuple()),
        1.0,
        id="No values",
    ),
    pytest.param(
        SplitRow(key=("a", "b"), values=tuple()),
        SplitRow(key=("a", "b"), values=tuple()),
        1.0,
        id="No values",
    ),
    pytest.param(
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(1.0,)),
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(1.0,)),
        1.0,
        id="Identity",
    ),
    pytest.param(
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(0.9,)),
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(1.0,)),
        0.9,
        id="90% match",
    ),
    pytest.param(
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(1.0, 0.0)),
        SplitRow(key=("a", datetime(2022, 1, 1, 0, 0)), values=(1.0, 1.0)),
        0.5,
        id="50% Match",
    ),
]


@pytest.mark.parametrize(
    "row1, row2, expected", TESTS,
)
def test_similarities(row1: SplitRow, row2: SplitRow, expected: float) -> None:
    assert abs(row_similarity(row1, row2) - expected) < 0.0001


TESTS = [
    pytest.param([], [], 1.0, id="Empty results",),
    pytest.param(
        [{"key": "asd", "value1": 1.0, "value2": 1.0}],
        [],
        0.0,
        id="Only result 1 has data",
    ),
    pytest.param(
        [],
        [{"key": "asd", "value1": 1.0, "value2": 1.0}],
        0.0,
        id="Only result 2 has data",
    ),
    pytest.param(
        [
            {"key": "asd", "value1": 1.0, "value2": 1.0},
            {"key": "asd2", "value1": 2.0, "value2": 3.0},
        ],
        [
            {"key": "asd", "value1": 1.0, "value2": 1.0},
            {"key": "asd2", "value1": 2.0, "value2": 3.0},
        ],
        1.0,
        id="Perfect match",
    ),
    pytest.param(
        [
            {"key": "asd", "value1": 1.0, "value2": 1.0},
            {"key": "asd", "value1": 2.0, "value2": 3.0},
            {"key": "asd", "value1": 5.0, "value2": 6.0},
        ],
        [
            {"key": "asd", "value1": 5.0, "value2": 6.0},
            {"key": "asd", "value1": 2.0, "value2": 3.0},
            {"key": "asd", "value1": 1.0, "value2": 1.0},
        ],
        1.0,
        id="Perfect match reverse order and multiple match per key",
    ),
    pytest.param(
        [
            {"key": "asd", "value1": 1.0, "value2": 1.0},
            {"key": "asd2", "value1": 2.0, "value2": 3.0},
        ],
        [
            {"key": "asd", "value1": 0.5, "value2": 0.5},
            {"key": "asd2", "value1": 1.0, "value2": 1.5},
        ],
        0.5,
        id="50% match",
    ),
    pytest.param(
        [
            {"key": "asd", "value1": 1.0, "value2": 1.0},
            {"key": "asd", "value1": 20.0, "value2": 30.0},
            {"key": "asd", "value1": 500.0, "value2": 600.0},
        ],
        [
            {"key": "asd", "value1": 250.0, "value2": 300.0},
            {"key": "asd", "value1": 10.0, "value2": 15.0},
            {"key": "asd", "value1": 0.5, "value2": 0.5},
        ],
        0.5,
        id="0.5 match reverse order and multiple match per key",
    ),
    pytest.param(
        [
            {"key": "asd", "value1": 0.0, "value2": 0.0},
            {"key": "asd2", "value1": 0.0, "value2": 0.0},
        ],
        [
            {"key": "asd", "value1": 0.0, "value2": 0.0},
            {"key": "asd2", "value1": 0.0, "value2": 0.0},
        ],
        1.0,
        id="Match with 0.0",
    ),
]


@pytest.mark.parametrize(
    "data_row1, data_row2, expected", TESTS,
)
def test_similarity_results(
    data_row1: MutableSequence[Row], data_row2: MutableSequence[Row], expected: float,
) -> None:
    schema = SplitSchema(
        complete=True, key_cols=("key",), value_cols=("value1", "value2"),
    )
    assert abs(calculate_score(data_row1, data_row2, schema) - expected) < 0.0001
