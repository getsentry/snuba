import pytest
from click.testing import CliRunner

from snuba.cli.jobs import JOB_SPECIFICATION_ERROR_MSG, run, run_from_manifest, status


@pytest.mark.redis_db
def test_cmd_line_valid() -> None:
    runner = CliRunner()
    result = runner.invoke(
        run,
        ["--job_type", "ToyJob", "--job_id", "0001"],
    )

    assert result.exit_code == 0


@pytest.mark.redis_db
def test_invalid_job_errors() -> None:
    runner = CliRunner()
    result = runner.invoke(
        run,
        [
            "--job_type",
            "NonexistentJob",
            "--job_id",
            "0001",
            "k1=v1",
            "k2=v2",
        ],
    )

    assert result.exit_code == 1


@pytest.mark.redis_db
def test_cmd_line_no_job_specification_errors() -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["k1=v1", "k2=v2"])
    assert result.exit_code == 1
    assert result.output == "Error: " + JOB_SPECIFICATION_ERROR_MSG + "\n"


@pytest.mark.redis_db
def test_cmd_line_no_job_id_errors() -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--job_type", "ToyJob", "k1=v1", "k2=v2"])
    assert result.exit_code == 1
    assert result.output == "Error: " + JOB_SPECIFICATION_ERROR_MSG + "\n"


@pytest.mark.redis_db
def test_cmd_line_no_job_type_errors() -> None:
    runner = CliRunner()
    result = runner.invoke(run, ["--job_id", "0001", "k1=v1", "k2=v2"])
    assert result.exit_code == 1
    assert result.output == "Error: " + JOB_SPECIFICATION_ERROR_MSG + "\n"


@pytest.mark.redis_db
def test_json_valid() -> None:
    runner = CliRunner()
    result = runner.invoke(
        run_from_manifest,
        [
            "--json_manifest",
            "job_manifest.json",
            "--job_id",
            "abc1234",
        ],
    )
    assert result.exit_code == 0


@pytest.mark.redis_db
def test_jobs_status() -> None:
    runner = CliRunner()
    runner.invoke(
        run_from_manifest,
        [
            "--json_manifest",
            "job_manifest.json",
            "--job_id",
            "abc1234",
        ],
    )
    result = runner.invoke(status, ["--job_id", "abc1234"])
    assert result.exit_code == 0
