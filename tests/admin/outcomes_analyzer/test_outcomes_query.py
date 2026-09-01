from unittest import mock

import pytest
from sentry_relay.consts import DataCategory

from snuba.admin.clickhouse.common import InvalidCustomQuery, format_predefined_sql
from snuba.admin.clickhouse.predefined_outcomes_queries import OutcomesQuery
from snuba.admin.outcomes_analyzer.enums import (
    Outcome,
    data_category_options,
    outcome_options,
    outcomes_enum_options,
)
from snuba.admin.outcomes_analyzer.outcomes_analyzer import (
    _allowed_tables,
    _stringify_result,
    run_outcomes_query,
)
from snuba.clickhouse.pool import ClickhouseResult


def test_predefined_outcomes_queries_registered() -> None:
    names = {cls.__name__ for cls in OutcomesQuery.all_classes()}
    assert {
        "VolumeByCategoryOverTime",
        "TopOrgsByCategory",
        "OrgVolumeByReason",
    }.issubset(names)


def test_outcomes_enum_options_from_python_modules() -> None:
    categories = data_category_options()
    outcomes = outcome_options()
    payload = outcomes_enum_options()

    assert payload["categories"] == categories
    assert payload["outcomes"] == outcomes

    category_values = {option["value"] for option in categories}
    assert str(int(DataCategory.ERROR)) in category_values
    assert str(int(DataCategory.REPLAY)) in category_values
    assert str(int(DataCategory.SPAN)) in category_values
    assert str(int(DataCategory.UNKNOWN)) not in category_values

    assert {"0 — accepted", "2 — rate_limited", "6 — cardinality_limited"}.issubset(
        {option["label"] for option in outcomes}
    )
    assert [option["value"] for option in outcomes] == [str(int(member)) for member in Outcome]


def test_predefined_query_json_shape() -> None:
    payload = next(cls.to_json() for cls in OutcomesQuery.all_classes())
    assert set(payload.keys()) == {"sql", "description", "name"}
    assert "{{" in payload["sql"]
    # Backend strips class-body indent so the FE can use sql as-is.
    assert not payload["sql"].startswith(" ")
    assert payload["sql"].startswith("SELECT")


def test_format_predefined_sql_preserves_inner_indent() -> None:
    raw = """
    SELECT
        org_id,
        sum(quantity)
    FROM outcomes_hourly_dist
    """
    formatted = format_predefined_sql(raw)
    assert formatted == "SELECT\n    org_id,\n    sum(quantity)\nFROM outcomes_hourly_dist"


def test_allowed_tables_include_hourly_dist() -> None:
    tables = _allowed_tables()
    assert "outcomes_hourly_dist" in tables
    assert "outcomes_hourly_local" in tables
    assert "outcomes_daily_dist_v2" in tables


def test_stringify_result() -> None:
    result = ClickhouseResult([[420217, 1320196], [215831, 373554]])
    assert _stringify_result(result) == ClickhouseResult(
        [["420217", "1320196"], ["215831", "373554"]]
    )


@mock.patch("snuba.admin.outcomes_analyzer.outcomes_analyzer.get_ro_query_node_connection")
def test_rejects_disallowed_table(mock_conn: mock.MagicMock) -> None:
    mock_pool = mock.MagicMock()
    mock_pool.execute_explain.return_value = ClickhouseResult(
        results=[("TABLE id: 0, table_name: system.parts",)]
    )
    mock_conn.return_value = mock_pool
    with pytest.raises(InvalidCustomQuery):
        run_outcomes_query("SELECT count() FROM system.parts", "test@sentry.io")


def test_rejects_non_select() -> None:
    with pytest.raises(InvalidCustomQuery):
        run_outcomes_query(
            "ALTER TABLE outcomes_hourly_dist DELETE WHERE 1",
            "test@sentry.io",
        )


@mock.patch("snuba.admin.outcomes_analyzer.outcomes_analyzer.get_ro_query_node_connection")
def test_allows_hourly_dist_query(mock_conn: mock.MagicMock) -> None:
    mock_pool = mock.MagicMock()
    mock_pool.execute_explain.return_value = ClickhouseResult(
        results=[("TABLE id: 0, table_name: outcomes_hourly_dist",)]
    )
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
