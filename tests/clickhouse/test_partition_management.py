from collections.abc import Callable
from unittest.mock import Mock, call

import pytest

from snuba.clickhouse.partition_management import (
    attach_partition_from_table,
    attach_partitions_from_table,
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
    health_check = Mock(spec=Callable[[], None])
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
    assert health_check.call_count == 2
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
    health_check = Mock(spec=Callable[[], None])

    result = attach_partitions_from_table(
        clickhouse,
        "default",
        "source",
        "destination",
        health_check=health_check,
        dry_run=True,
    )

    assert result == ["two"]
    health_check.assert_not_called()
    assert clickhouse.execute.call_count == 2


def test_rejects_attaching_a_table_to_itself() -> None:
    clickhouse = Mock(spec=ClickhousePool)

    with pytest.raises(ValueError, match="must be different"):
        attach_partitions_from_table(clickhouse, "default", "source", "source")

    clickhouse.execute.assert_not_called()
