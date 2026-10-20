from collections.abc import Callable
from datetime import datetime, timedelta
from unittest.mock import Mock, call

import pytest

from snuba.clickhouse.partition_management import (
    PartitionBoundaryError,
    UnhealthyError,
    attach_partition_from_table,
    attach_partitions_from_table,
    build_health_check,
    filter_expired_partitions,
    get_partition_boundaries,
    is_partition_too_far_in_future,
    parse_partition_start,
    parse_retention_days,
    run_health_check_query,
)
from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult


def make_clickhouse(source_partitions: list[str], destination_partitions: list[str]) -> Mock:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.side_effect = [
        ClickhouseResult([(partition,) for partition in source_partitions]),
        ClickhouseResult([(partition,) for partition in destination_partitions]),
        *[ClickhouseResult() for _ in source_partitions],
    ]
    return clickhouse


def test_attaches_missing_partitions_one_at_a_time() -> None:
    clickhouse = make_clickhouse(["202401", "202402", "202403"], ["202402"])
    health_check = Mock(spec=Callable[[str], None])
    attached = Mock(spec=Callable[[str], None])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=health_check,
        on_partition_attached=attached,
    )

    assert result == ["202401", "202403"]
    assert health_check.call_args_list == [call("202401"), call("202403")]
    assert attached.call_args_list == [call("202401"), call("202403")]
    assert clickhouse.execute.call_args_list[2:] == [
        call(
            "ALTER TABLE default.destination ATTACH PARTITION ID "
            "%(partition_id)s FROM default.source",
            {"partition_id": "202401"},
        ),
        call(
            "ALTER TABLE default.destination ATTACH PARTITION ID "
            "%(partition_id)s FROM default.source",
            {"partition_id": "202403"},
        ),
    ]


def test_attaches_a_single_partition_without_discovery() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult()

    attach_partition_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        "202402",
    )

    clickhouse.execute.assert_called_once_with(
        "ALTER TABLE default.destination ATTACH PARTITION ID %(partition_id)s FROM default.source",
        {"partition_id": "202402"},
    )


def test_stops_before_next_partition_when_health_check_fails() -> None:
    clickhouse = make_clickhouse(["one", "two"], [])
    health_check = Mock(side_effect=[None, RuntimeError("unhealthy")])

    with pytest.raises(RuntimeError, match="unhealthy"):
        attach_partitions_from_table(
            clickhouse,
            "default",
            "source",
            "destination",
            health_check=health_check,
            max_future_age=None,
        )

    assert clickhouse.execute.call_args_list[2:] == [
        call(
            "ALTER TABLE default.destination ATTACH PARTITION ID "
            "%(partition_id)s FROM default.source",
            {"partition_id": "one"},
        )
    ]


def test_dry_run_does_not_check_health_or_attach() -> None:
    clickhouse = make_clickhouse(["one", "two"], ["one"])
    health_check = Mock(spec=Callable[[str], None])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=health_check,
        max_future_age=None,
        dry_run=True,
    )

    assert result == ["two"]
    health_check.assert_not_called()
    assert clickhouse.execute.call_count == 2


@pytest.mark.parametrize(
    "partition,expected",
    [
        # Compact partition IDs from a date-only key.
        ("2024", datetime(2024, 1, 1)),
        ("202402", datetime(2024, 2, 1)),
        ("20240213", datetime(2024, 2, 13)),
        ("2024021307", datetime(2024, 2, 13, 7)),
        ("20240213070405", datetime(2024, 2, 13, 7, 4, 5)),
        # Rendered partition values, as reported by system.parts.
        ("2024-02-12", datetime(2024, 2, 12)),
        ("'2024-02-12'", datetime(2024, 2, 12)),
        ("2024-02-12 03:00:00", datetime(2024, 2, 12, 3)),
        # eap_items partitions by (retention_days, toMonday(timestamp)).
        ("(90,'2024-02-12')", datetime(2024, 2, 12)),
        ("(30,'2024-02-12')", datetime(2024, 2, 12)),
        # The date component is found regardless of its position in the tuple.
        ("('2024-02-12',90)", datetime(2024, 2, 12)),
        # A String key makes the partition ID a hash, but the value still reads.
        ("('prod','2024-02-12')", datetime(2024, 2, 12)),
    ],
)
def test_parses_the_start_of_a_partition_boundary(partition: str, expected: datetime) -> None:
    assert parse_partition_start(partition) == expected


@pytest.mark.parametrize("partition", ["all", "tuple()", "(90)", ""])
def test_rejects_partitions_without_a_time_boundary(partition: str) -> None:
    with pytest.raises(PartitionBoundaryError, match="Cannot derive a partition start"):
        parse_partition_start(partition)


def test_health_check_query_receives_the_partition_start() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult([(1,)])

    run_health_check_query(
        clickhouse,
        "SELECT count() FROM destination WHERE timestamp >= %(partition_start)s",
        "202402",
    )

    clickhouse.execute.assert_called_once_with(
        "SELECT count() FROM destination WHERE timestamp >= %(partition_start)s",
        {"partition_start": datetime(2024, 2, 1)},
    )


def test_health_check_query_prefers_the_partition_value_over_a_hashed_id() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult([(1,)])

    run_health_check_query(
        clickhouse,
        "SELECT count() FROM destination WHERE timestamp >= %(partition_start)s",
        "940d0b6721e369375f11224b94ef5362",
        "('prod','2024-02-12')",
    )

    assert clickhouse.execute.call_args.args[1] == {"partition_start": datetime(2024, 2, 12)}


def test_resolves_boundaries_from_the_source_table_for_hashed_partition_ids() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.side_effect = [
        # Source and destination partition discovery.
        ClickhouseResult([("90-20240212",), ("90-20240304",)]),
        ClickhouseResult([("90-20240304",)]),
        # Boundary lookup against the source table.
        ClickhouseResult(
            [("90-20240212", "(90,'2024-02-12')"), ("90-20240304", "(90,'2024-03-04')")]
        ),
        # Health check, then the attach itself.
        ClickhouseResult([(1,)]),
        ClickhouseResult(),
    ]

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check_query="SELECT count() FROM destination WHERE timestamp >= %(partition_start)s",
        # These 2024 partitions are long past retention; expiry is tested separately.
        retention_margin=None,
    )

    assert result == ["90-20240212"]
    health_check_call = clickhouse.execute.call_args_list[3]
    assert health_check_call.args[1] == {
        "partition_start": datetime(2024, 2, 12),
        "retention_days": 90,
    }


@pytest.mark.parametrize(
    "partition,expected",
    [
        ("(90,'2024-02-12')", 90),
        ("(30,'2024-02-12')", 30),
        ("('2024-02-12',90)", 90),
        # No retention component to read.
        ("2024-02-12", None),
        ("'2024-02-12'", None),
        # A bare integer here is the partition ID, not a key component.
        ("202402", None),
        ("20240213", None),
        # Ambiguous: more than one candidate integer.
        ("(90,7,'2024-02-12')", None),
    ],
)
def test_parses_retention_days_from_a_partition(partition: str, expected: int | None) -> None:
    assert parse_retention_days(partition) == expected


def test_health_check_query_binds_retention_days_alongside_the_boundary() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult([(1,)])

    run_health_check_query(
        clickhouse,
        "SELECT count() FROM destination "
        "WHERE timestamp >= %(partition_start)s AND retention_days = %(retention_days)s",
        "90-20240212",
        "(90,'2024-02-12')",
    )

    assert clickhouse.execute.call_args.args[1] == {
        "partition_start": datetime(2024, 2, 12),
        "retention_days": 90,
    }


def test_health_check_query_omits_retention_days_when_the_key_has_none() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult([(1,)])

    run_health_check_query(
        clickhouse,
        "SELECT count() FROM destination WHERE timestamp >= %(partition_start)s",
        "20240212",
        "2024-02-12",
    )

    assert clickhouse.execute.call_args.args[1] == {"partition_start": datetime(2024, 2, 12)}


def test_rejects_a_query_referencing_an_unavailable_parameter() -> None:
    clickhouse = Mock(spec=ClickhousePool)

    with pytest.raises(PartitionBoundaryError, match="retention_days"):
        run_health_check_query(
            clickhouse,
            "SELECT 1 FROM destination WHERE retention_days = %(retention_days)s",
            "20240212",
            "2024-02-12",
        )

    clickhouse.execute.assert_not_called()


def test_retention_days_distinguishes_partitions_sharing_a_boundary() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.side_effect = [
        ClickhouseResult([("30-20240212",), ("90-20240212",)]),
        ClickhouseResult([]),
        ClickhouseResult(
            [("30-20240212", "(30,'2024-02-12')"), ("90-20240212", "(90,'2024-02-12')")]
        ),
        ClickhouseResult([(1,)]),
        ClickhouseResult(),
        ClickhouseResult([(1,)]),
        ClickhouseResult(),
    ]

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check_query=(
            "SELECT count() FROM destination "
            "WHERE timestamp >= %(partition_start)s AND retention_days = %(retention_days)s"
        ),
        # These 2024 partitions are long past retention; expiry is tested separately.
        retention_margin=None,
    )

    assert result == ["30-20240212", "90-20240212"]
    # Same boundary, but each check is scoped to its own retention.
    assert clickhouse.execute.call_args_list[3].args[1] == {
        "partition_start": datetime(2024, 2, 12),
        "retention_days": 30,
    }
    assert clickhouse.execute.call_args_list[5].args[1] == {
        "partition_start": datetime(2024, 2, 12),
        "retention_days": 90,
    }


def test_reads_partition_boundaries_for_a_table() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult(
        [("90-20240212", "(90,'2024-02-12')"), ("30-20240212", "(30,'2024-02-12')")]
    )

    assert get_partition_boundaries(clickhouse, "default", "eap_items_local") == {
        "90-20240212": "(90,'2024-02-12')",
        "30-20240212": "(30,'2024-02-12')",
    }


@pytest.mark.parametrize("results", [[], [()], [(0,)], [(None,)]])
def test_health_check_query_treats_an_empty_result_as_unhealthy(
    results: list[tuple[object, ...]],
) -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult(results)

    with pytest.raises(UnhealthyError, match="reported unhealthy"):
        run_health_check_query(clickhouse, "SELECT 0", "202402")


def test_health_check_query_aborts_before_the_first_attach() -> None:
    clickhouse = make_clickhouse(["202401", "202402"], [])
    clickhouse.execute.side_effect = [
        ClickhouseResult([("202401",), ("202402",)]),
        ClickhouseResult([]),
        ClickhouseResult([(0,)]),
    ]

    with pytest.raises(UnhealthyError, match="partition 202401"):
        attach_partitions_from_table(
            clickhouse,
            "default",
            "source",
            "destination",
            health_check=build_health_check(clickhouse, "SELECT 0"),
        )

    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "ATTACH PARTITION" in call_args.args[0]
    ]


def test_default_health_check_ignores_the_partition_id() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult()

    build_health_check(clickhouse, None)("an-opaque-partition-id")

    clickhouse.execute.assert_called_once_with("SELECT 1")


def test_rejects_attaching_a_table_to_itself() -> None:
    clickhouse = Mock(spec=ClickhousePool)

    with pytest.raises(ValueError, match="must be different"):
        attach_partitions_from_table(clickhouse, "default", "source", "source")

    clickhouse.execute.assert_not_called()


@pytest.mark.parametrize(
    "partition,expected",
    [
        # Well inside the horizon.
        ("20240213", False),
        # The current partition, and one already in the past.
        ("20240220", False),
        ("20240101", False),
        # Exactly at the horizon is kept; only partitions beyond it are skipped.
        ("20240305", False),
        # Beyond two weeks.
        ("20240306", True),
        ("20250213", True),
        # Rendered tuple values are compared on their date component.
        ("(90,'2024-03-25')", True),
        ("(90,'2024-02-12')", False),
        # An unreadable boundary is not treated as future-dated, which leaves
        # the explicit error to the health check path.
        ("an-opaque-partition-id", False),
    ],
)
def test_identifies_partitions_dated_too_far_in_the_future(partition: str, expected: bool) -> None:
    assert is_partition_too_far_in_future(partition, now=datetime(2024, 2, 20)) is expected


def test_future_horizon_is_configurable() -> None:
    # Beyond the two week default, but inside a wider horizon.
    partition = "20240310"

    assert is_partition_too_far_in_future(partition, now=datetime(2024, 2, 20)) is True
    assert (
        is_partition_too_far_in_future(
            partition, now=datetime(2024, 2, 20), max_future_age=timedelta(weeks=8)
        )
        is False
    )


def test_skips_future_dated_partitions_when_attaching() -> None:
    clickhouse = make_clickhouse(["20240213", "20240220", "20250213"], [])
    skipped = Mock(spec=Callable[[str, str], None])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        on_partition_skipped=skipped,
        now=datetime(2024, 2, 20),
    )

    assert result == ["20240213", "20240220"]
    assert skipped.call_args_list == [call("20250213", "20250213")]
    assert [
        call_args.args[1]["partition_id"]
        for call_args in clickhouse.execute.call_args_list
        if "ATTACH PARTITION" in call_args.args[0]
    ] == ["20240213", "20240220"]


def test_parseable_partition_ids_need_no_boundary_query() -> None:
    clickhouse = make_clickhouse(["20240213", "20250213"], [])

    attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        now=datetime(2024, 2, 20),
    )

    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "partition_id, partition" in call_args.args[0]
    ]


def test_resolves_hashed_partition_ids_before_date_filtering() -> None:
    # A String column in the partition key hashes the ID, so the future date is
    # only visible through the rendered partition value.
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.side_effect = [
        ClickhouseResult([("hash-past",), ("hash-future",)]),
        ClickhouseResult([]),
        ClickhouseResult(
            [
                ("hash-past", "('prod','2024-02-12')"),
                ("hash-future", "('prod','2025-02-12')"),
            ]
        ),
        ClickhouseResult(),
    ]

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        now=datetime(2024, 2, 20),
    )

    assert result == ["hash-past"]


def test_attaches_future_partitions_when_the_filter_is_disabled() -> None:
    clickhouse = make_clickhouse(["20240213", "20250213"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        max_future_age=None,
        now=datetime(2024, 2, 20),
    )

    assert result == ["20240213", "20250213"]


def test_dry_run_reports_the_date_filtered_partitions() -> None:
    clickhouse = make_clickhouse(["20240213", "20250213"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        now=datetime(2024, 2, 20),
        dry_run=True,
    )

    assert result == ["20240213"]
    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "ATTACH PARTITION" in call_args.args[0]
    ]


WEEKLY_RETENTION_KEY = "(retention_days, toMonday(timestamp))"
# 2026-08-31 + 7 days + 30 days retention + 14 days margin = 2026-10-21.
EXPIRY_NOW = datetime(2026, 10, 22)


def make_eap_clickhouse(
    source_partitions: list[str],
    destination_partitions: list[str],
    partition_key: str = WEEKLY_RETENTION_KEY,
) -> Mock:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.side_effect = [
        ClickhouseResult([(partition,) for partition in source_partitions]),
        ClickhouseResult([(partition,) for partition in destination_partitions]),
        ClickhouseResult([(partition_key,)]),
        *[ClickhouseResult() for _ in source_partitions],
    ]
    return clickhouse


def test_skips_partitions_past_retention() -> None:
    clickhouse = make_eap_clickhouse(
        ["30-20260831", "30-20260907", "90-19700101", "90-20260831"], []
    )
    expired = Mock(spec=Callable[[str], None])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        on_partition_expired=expired,
        max_future_age=None,
        now=EXPIRY_NOW,
    )

    # 30-20260907 and the 90 day week are still within retention plus margin.
    assert result == ["30-20260907", "90-20260831"]
    assert expired.call_args_list == [call("30-20260831"), call("90-19700101")]
    assert clickhouse.execute.call_args_list[2] == call(
        "SELECT partition_key FROM system.tables WHERE database = %(database)s AND name = %(table)s",
        {"database": "default", "table": "source"},
    )


def test_expiry_uses_the_retention_margin() -> None:
    clickhouse = make_eap_clickhouse(["30-20260831"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        retention_margin=timedelta(days=30),
        max_future_age=None,
        now=EXPIRY_NOW,
    )

    assert result == ["30-20260831"]


def test_attaches_expired_partitions_when_the_filter_is_disabled() -> None:
    clickhouse = make_clickhouse(["30-20260831", "90-19700101"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        retention_margin=None,
        max_future_age=None,
        now=EXPIRY_NOW,
    )

    assert result == ["30-20260831", "90-19700101"]
    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "system.tables" in call_args.args[0]
    ]


def test_keeps_expired_looking_partitions_of_other_partition_keys() -> None:
    # A monthly key renders the same ID shape, but the ID can't be read as a week.
    clickhouse = make_eap_clickhouse(
        ["30-20260801"], [], partition_key="(retention_days, toStartOfMonth(timestamp))"
    )

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        max_future_age=None,
        now=EXPIRY_NOW,
    )

    assert result == ["30-20260801"]


def test_does_not_read_the_partition_key_without_expired_candidates() -> None:
    clickhouse = make_clickhouse(["30-20261012", "20240213"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=Mock(spec=Callable[[str], None]),
        max_future_age=None,
        now=EXPIRY_NOW,
    )

    assert result == ["30-20261012", "20240213"]
    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "system.tables" in call_args.args[0]
    ]


def test_dry_run_reports_the_expiry_filtered_partitions() -> None:
    clickhouse = make_eap_clickhouse(["30-20260831", "30-20260907"], [])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        max_future_age=None,
        now=EXPIRY_NOW,
        dry_run=True,
    )

    assert result == ["30-20260907"]
    assert not [
        call_args
        for call_args in clickhouse.execute.call_args_list
        if "ATTACH PARTITION" in call_args.args[0]
    ]


def test_filter_expired_partitions_skips_missing_tables() -> None:
    clickhouse = Mock(spec=ClickhousePool)
    clickhouse.execute.return_value = ClickhouseResult([])

    result = filter_expired_partitions(
        clickhouse,
        "default",
        "missing",
        ["90-19700101"],
        today=EXPIRY_NOW.date(),
    )

    assert result == ["90-19700101"]
