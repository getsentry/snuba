from collections.abc import Mapping
from typing import Any
from unittest.mock import Mock, patch

from click.testing import CliRunner

from snuba.cli.attach_partitions import attach_partitions
from snuba.clickhouse.pool import ClickhousePool, ClickhouseResult

EXPIRED = "90-19700101"


def make_connection() -> Mock:
    def execute(query: str, params: Mapping[str, Any] | None = None) -> ClickhouseResult:
        if "FROM system.tables" in query:
            return ClickhouseResult([("(retention_days, toMonday(timestamp))",)])
        if "FROM system.parts" in query:
            return ClickhouseResult([(EXPIRED, "(90,'1970-01-01')")])
        raise AssertionError(f"unexpected query: {query}")

    connection = Mock(spec=ClickhousePool)
    connection.execute.side_effect = execute
    return connection


def invoke(connection: Mock, *args: str) -> Any:
    with patch("snuba.cli.attach_partitions.build_pool", return_value=connection):
        return CliRunner().invoke(
            attach_partitions,
            [
                "eap_items_1_local",
                "eap_items_2_local",
                "--storage",
                "eap_items",
                "--clickhouse-host",
                "localhost",
                "--clickhouse-port",
                "8123",
                "--partition-id",
                EXPIRED,
                *args,
            ],
        )


def test_refuses_a_single_expired_partition() -> None:
    connection = make_connection()

    result = invoke(connection, "--execute")

    assert result.exit_code == 2, result.output
    assert "past retention" in result.output
    assert "--include-expired" in result.output
    assert not [c for c in connection.execute.call_args_list if "ATTACH" in c.args[0]]


def test_include_expired_allows_a_single_expired_partition() -> None:
    connection = make_connection()

    result = invoke(connection, "--include-expired")

    assert result.exit_code == 0, result.output
    assert not [c for c in connection.execute.call_args_list if "system.tables" in c.args[0]]
