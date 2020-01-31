import pytest

from snuba import settings, state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.tables import TableSource
from snuba.query.parser import parse_query
from snuba.query.processors.prewhere import CustomPrewhereProcessor, PrewhereProcessor
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


test_custom_prewhere = [
    (
        {
            "conditions": [
                ["project_id", "IN", [4]],
                ["d", "=", "3"],
                ["type", "=", "error"],
                ["event_id", "=", "1"],
                ["b", "=", "2"],
            ],
        },
        [["d", "=", "3"], ["type", "=", "error"], ["b", "=", "2"]],
        [["event_id", "=", "1"], ["project_id", "IN", [4]]],
    ),
    (
        {
            "conditions": [
                ["project_id", "IN", [1, 3]],
                ["d", "=", "3"],
                ["type", "=", "error"],
                ["event_id", "=", "1"],
                ["b", "=", "2"],
            ],
        },
        [["d", "=", "3"], ["b", "=", "2"]],
        [["type", "=", "error"], ["event_id", "=", "1"], ["project_id", "IN", [1, 3]]],
    ),
]


@pytest.mark.parametrize(
    "query_body, new_conditions, prewhere_conditions", test_custom_prewhere
)
def test_custom_prewhere(query_body, new_conditions, prewhere_conditions) -> None:
    settings.MAX_PREWHERE_CONDITIONS = 3
    state.set_config("prewhere_custom_key_projects", "[1,3]")

    dataset = get_dataset("events")
    query = parse_query(query_body, dataset)
    request_settings = HTTPRequestSettings()

    processor = CustomPrewhereProcessor(custom_candidates=["type"])
    processor.process_query(query, request_settings)

    assert query.get_conditions() == new_conditions
    assert query.get_prewhere() == prewhere_conditions
