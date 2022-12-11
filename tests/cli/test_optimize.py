from click.testing import CliRunner

from snuba.cli.optimize import optimize


def test_optimize_cli() -> None:
    runner = CliRunner()
    runner.invoke(optimize, ["--parallel", "2", "--storage", "errors"])
