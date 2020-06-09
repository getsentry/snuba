import pytest

from typing import Any, Sequence

from snuba.query.logical import Groupby, Query
from snuba.query.types import Condition
from snuba.query.processors.join_optimizers import SimpleJoinOptimizer
from snuba.request.request_settings import HTTPRequestSettings
from tests.datasets.schemas.join_examples import simple_join_structure

test_data = [
    (
        ["t1.t1c1", "t2.t2c2"],
        [["t1.t2c2", "=", "a"]],
        [],
        "table1 t1 INNER JOIN table2 t2 ON t1.t1c1 = t2.t2c2 AND t1.t1c3 = t2.t2c4",
    ),
    (["t1.t1c1"], [["t1.t1c2", "=", "a"]], [], "table1 t1"),
    (
        ["t1.t1c1"],
        [["t2.t1c1", "=", "a"]],
        [],
        "table1 t1 INNER JOIN table2 t2 ON t1.t1c1 = t2.t2c2 AND t1.t1c3 = t2.t2c4",
    ),
    (
        ["t1.t1c1"],
        [],
        ["t2.t2c2"],
        "table1 t1 INNER JOIN table2 t2 ON t1.t1c1 = t2.t2c2 AND t1.t1c3 = t2.t2c4",
    ),
]


@pytest.mark.parametrize("selected_cols, conditions, groupby, expected", test_data)
def test_join_optimizer_two_tables(
    selected_cols: Sequence[Any],
    conditions: Sequence[Condition],
    groupby: Groupby,
    expected: str,
) -> None:
    query = Query(
        {
            "selected_columns": selected_cols,
            "conditions": conditions,
            "arrayjoin": None,
            "having": [],
            "groupby": groupby,
            "aggregations": [],
            "orderby": None,
            "limitby": None,
            "sample": 10,
            "limit": 100,
            "offset": 50,
            "totals": True,
            "granularity": 60,
        },
        simple_join_structure,
    )
    request_settings = HTTPRequestSettings()

    optimizer = SimpleJoinOptimizer()
    optimizer.process_query(query, request_settings)

    assert query.get_data_source().format_from() == expected
