from click.testing import CliRunner

from snuba.cli.jobs import jobs


def test_dry_run_cli() -> None:
    runner = CliRunner()
    result = runner.invoke(jobs, ["--dry_run", "--storage_name", "doesntmatter"])
    assert result.exit_code == 0
