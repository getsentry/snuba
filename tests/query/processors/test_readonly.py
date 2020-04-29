import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet, String
from snuba.datasets.schemas.tables import TableSource
from snuba.query.logical import Query
from snuba.query.processors.readonly_events import ReadOnlyTableSelector
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    ("sentry_dist", True, "sentry_dist"),
    ("sentry_dist", False, "sentry_ro"),
    ("bla", False, "bla"),
]


@pytest.mark.parametrize("initial_table, consistent, expected_table", test_data)
def test_prewhere(initial_table, consistent, expected_table) -> None:
    state.set_config("enable_events_readonly_table", True)
    body = {
        "conditions": [
            ["d", "=", "1"],
            ["c", "=", "3"],
            ["a", "=", "1"],
            ["b", "=", "2"],
        ],
    }
    cols = ColumnSet([("col", String())])
    query = Query(body, TableSource(initial_table, cols, [["time", "=", "1"]], ["c1"]),)

    request_settings = HTTPRequestSettings(consistent=consistent)
    processor = ReadOnlyTableSelector("sentry_dist", "sentry_ro")
    processor.process_query(query, request_settings)

    source = query.get_data_source()
    assert isinstance(source, TableSource)
    assert source.format_from() == expected_table
    assert source.get_columns() == cols
    assert source.get_prewhere_candidates() == ["c1"]
    assert source.get_mandatory_conditions() == [["time", "=", "1"]]
