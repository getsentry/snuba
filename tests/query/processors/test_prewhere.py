import pytest

from snuba import settings
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


test_data = [
    (
        {"conditions": [[["positionCaseInsensitive", ["message", "abc"]], "!=", 0]]},
        [
            "event_id",
            "group_id",
            "tags[sentry:release]",
            "message",
            "environment",
            "project_id",
        ],
        [],
        [[["positionCaseInsensitive", ["message", "abc"]], "!=", 0]],
    ),
    (
        # Add pre-where condition in the expected order
        {
            "conditions": [
                ["d", "=", "1"],
                ["c", "=", "3"],
                ["a", "=", "1"],
                ["b", "=", "2"],
            ],
        },
        ["a", "b", "c"],
        [["d", "=", "1"], ["c", "=", "3"]],
        [["a", "=", "1"], ["b", "=", "2"]],
    ),
    (
        # Do not add conditions that are parts of an OR
        {"conditions": [[["a", "=", "1"], ["b", "=", "2"]], ["c", "=", "3"]]},
        ["a", "b", "c"],
        [[["a", "=", "1"], ["b", "=", "2"]]],
        [["c", "=", "3"]],
    ),
]


@pytest.mark.parametrize(
    "query_body, keys, new_conditions, prewhere_conditions", test_data
)
def test_prewhere(query_body, keys, new_conditions, prewhere_conditions) -> None:
    settings.MAX_PREWHERE_CONDITIONS = 2
    query = Query(query_body, TableSource("my_table", ColumnSet([]), None, keys),)

    request_settings = HTTPRequestSettings()
    processor = PrewhereProcessor()
    processor.process_query(query, request_settings)

    assert query.get_conditions() == new_conditions
    assert query.get_prewhere() == prewhere_conditions
