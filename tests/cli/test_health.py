from unittest import mock

from click.testing import CliRunner

from snuba.cli.health import health


def test_check_health() -> None:
    runner = CliRunner()
    # down file existing does not mean the pod is unhealthy
    with mock.patch("snuba.cli.health.check_down_file_exists", return_value=True):
        result = runner.invoke(health)
        assert result.exit_code == 0
    # don't check clickhouse if not thorough
    with mock.patch("snuba.cli.health.check_clickhouse", return_value=False):
        result = runner.invoke(health)
        assert result.exit_code == 0
    # thorough healthcheck fails on bad clickhouse connection
    with mock.patch("snuba.cli.health.check_clickhouse", return_value=False):
        result = runner.invoke(health, "--thorough")
        assert result.exit_code == 1
    # thorough healthcheck passes on good clickhouse connection
    with mock.patch("snuba.cli.health.check_clickhouse", return_value=True):
        result = runner.invoke(health, "--thorough")
        assert result.exit_code == 0
