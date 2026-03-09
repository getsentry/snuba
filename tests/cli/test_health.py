from unittest import mock

from click.testing import CliRunner

from snuba.cli.health import health


def test_down_file_exists_pod_healthy() -> None:
    runner = CliRunner()
    # down file existing does not mean the pod is unhealthy
    with mock.patch("snuba.utils.health_info.check_down_file_exists", return_value=True):
        with mock.patch(
            "snuba.utils.health_info.sanity_check_clickhouse_connections",
            return_value=True,
        ):
            result = runner.invoke(health)
            assert result.exit_code == 0


def test_bad_clickhouse_connection_healthcheck_fails() -> None:
    runner = CliRunner()
    # sanity check clickhouse connections when not running in thorough mode
    with mock.patch(
        "snuba.utils.health_info.sanity_check_clickhouse_connections",
        return_value=False,
    ):
        result = runner.invoke(health)
        assert result.exit_code == 1


def test_bad_clickhouse_connection_thorough_healthcheck_fails() -> None:
    runner = CliRunner()
    # thorough healthcheck fails on bad clickhouse connection, because we cannot verify
    # tables
    with mock.patch("snuba.utils.health_info.check_all_tables_present", return_value=False):
        result = runner.invoke(health, "--thorough")
        assert result.exit_code == 1


def test_good_clickhouse_connection_thorough_healthcheck_passes() -> None:
    runner = CliRunner()
    # thorough healthcheck passes on good clickhouse connection
    with mock.patch("snuba.utils.health_info.check_all_tables_present", return_value=True):
        result = runner.invoke(health, "--thorough")
        assert result.exit_code == 0
