from unittest import mock

import pytest

from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.predefined_outcomes_queries import OutcomesQuery
from snuba.admin.outcomes_analyzer.outcomes_analyzer import (
    _stringify_result,
    run_outcomes_query,
)
from snuba.clickhouse.native import ClickhouseResult


def test_predefined_outcomes_queries_registered() -> None:
    names = {cls.__name__ for cls in OutcomesQuery.all_classes()}
    assert "VolumeByCategoryOverTime" in names
    assert "TopOrgsByCategory" in names
    assert "OrgVolumeByReason" in names
    assert "TimeRangeTopOrgs" in names


def test_predefined_query_json_shape() -> None:
    payload = next(cls.to_json() for cls in OutcomesQuery.all_classes())
    assert set(payload.keys()) == {"sql", "description", "name"}
    assert "{{category}}" in payload["sql"] or "{{lookback_hours}}" in payload["sql"]


@pytest.mark.parametrize(
    "result,expected_result",
    [
        pytest.param(
            ClickhouseResult(
                [
                    [420217, 1320196],
                    [215831, 373554],
                ]
            ),
            ClickhouseResult(
                [
                    ["420217", "1320196"],
                    ["215831", "373554"],
                ]
            ),
            id="org_ids",
        ),
    ],
)
def test_stringify_result(result: ClickhouseResult, expected_result: ClickhouseResult) -> None:
    assert _stringify_result(result) == expected_result


def test_rejects_disallowed_table() -> None:
    with pytest.raises(InvalidCustomQuery):
        run_outcomes_query(
            "SELECT count() FROM system.parts",
            "test@sentry.io",
        )


def test_rejects_non_select() -> None:
    with pytest.raises(InvalidCustomQuery):
        run_outcomes_query(
            "ALTER TABLE outcomes_hourly_dist DELETE WHERE 1",
            "test@sentry.io",
        )


@mock.patch("snuba.admin.outcomes_analyzer.outcomes_analyzer.get_ro_query_node_connection")
def test_allows_hourly_dist_query(mock_conn: mock.MagicMock) -> None:
    mock_pool = mock.MagicMock()
    mock_pool.execute.return_value = ClickhouseResult(
        results=[[1]],
        meta=[("c", "UInt64")],
    )
    mock_conn.return_value = mock_pool

    result = run_outcomes_query(
        "SELECT sum(quantity) FROM outcomes_hourly_dist "
        "WHERE category = 7 AND timestamp >= now() - INTERVAL 24 HOUR",
        "test@sentry.io",
    )

    assert result.results == [["1"]]
    mock_pool.execute.assert_called_once()
