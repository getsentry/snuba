import pytest

from typing import Any, Mapping, MutableMapping

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.query import Query

test_data = [
    (
        # Basic one column change
        {
            "selected_columns": ["c1", "c2", ["f1", ["c1"]]],
            "conditions": [["c1", "=", "a"]],
        },
        "c1",
        "c3",
        {
            "selected_columns": ["c3", "c2", ["f1", ["c3"]]],
            "conditions": [["c3", "=", "a"]],
        },
    ),
    (
        # Test simple change in all fields
        {
            "selected_columns": ["c1", "c2", "c3"],
            "conditions": [["c1", "=", "a"]],
            "arrayjoin": "c1",
            "groupby": "c1",
            "aggregations": [["myAggregate()", "c1", "c1"]],
            "orderby": ["-c1"],
        },
        "c1",
        "c4",
        {
            "selected_columns": ["c4", "c2", "c3"],
            "conditions": [["c4", "=", "a"]],
            "arrayjoin": "c4",
            "groupby": ["c4"],
            "aggregations": [["myAggregate()", "c4", "c1"]],
            "orderby": ["-c4"],
        },
    ),
    (
        # Test complex nested function
        {
            "selected_columns": [
                [
                    "function",
                    [
                        ["another_function", ["column", "another_column"]],
                        "column",
                        "another_column",
                    ],
                    "column",  # alias
                ]
            ]
        },
        "column",
        "replaced",
        {
            "selected_columns": [
                [
                    "function",
                    [
                        ["another_function", ["replaced", "another_column"]],
                        "replaced",
                        "another_column",
                    ],
                    "column",
                ]
            ]
        },
    ),
    (
        # Test complex conditions with functions
        {
            "conditions": [
                ["column", "=", "a"],
                ["other_column", "=", "a"],
                [
                    ["column", "=", "b"],
                    ["other_column", "=", "a"],
                    [["function", ["column"]], "=", "b"],
                    [[[[[["column", "=", "c"]]], []]]],
                ],
            ],
        },
        "column",
        "replaced",
        {
            "conditions": [
                ["replaced", "=", "a"],
                ["other_column", "=", "a"],
                [
                    ["replaced", "=", "b"],
                    ["other_column", "=", "a"],
                    [["function", ["replaced"]], "=", "b"],
                    [[[[[["replaced", "=", "c"]]], []]]],
                ],
            ],
        },
    ),
    (
        # Test aggregate with complex expression
        {
            "aggregations": [
                ["myAggregate()", [["function", ["column", "another_column"]]], "c1"],
                ["myAggregate()", ["column", "another_column", "column"], "c1"],
            ],
        },
        "column",
        "replaced",
        {
            "aggregations": [
                ["myAggregate()", [["function", ["replaced", "another_column"]]], "c1"],
                ["myAggregate()", ["replaced", "another_column", "replaced"], "c1"],
            ],
        },
    ),
]


@pytest.mark.parametrize("initial_query, old_col, new_col, expected", test_data)
def test_col_replacement(
    initial_query: MutableMapping[str, Any],
    old_col: str,
    new_col: str,
    expected: Mapping[str, Any],
):
    query = Query(initial_query, TableSource("my_table", ColumnSet([])))
    query.replace_column(old_col, new_col)
    assert expected == query.get_body()
