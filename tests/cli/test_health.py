from click.testing import CliRunner

from snuba.cli.health import health


def test_healthcheck_passes() -> None:
    runner = CliRunner()
    result = runner.invoke(health)
    assert result.exit_code == 0


def test_thorough_healthcheck_passes() -> None:
    runner = CliRunner()
    result = runner.invoke(health, "--thorough")
    assert result.exit_code == 0
