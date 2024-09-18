from click.testing import CliRunner

from snuba.cli.jobs import run


def test_valid_job() -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["ToyJob", "--dry_run", "True", "k1=v1", "k2=v2"])
    assert result.exit_code == 0


def test_invalid_job() -> None:
    runner = CliRunner()
    result = runner.invoke(
        run, ["SomeJobThatDoesntExist", "--dry_run", "True", "k1=v1", "k2=v2"]
    )
    assert result.exit_code == 1
