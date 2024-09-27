import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.runner import JobStatus, get_job_status, run_job

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


@pytest.mark.redis_db
def test_job_status_changes_to_finished() -> None:
    assert get_job_status(JOB_ID) is None
    run_job(test_job_spec, False)
    assert get_job_status(JOB_ID) == JobStatus.FINISHED


@pytest.mark.redis_db
def test_job_status_with_invalid_job_id() -> None:
    assert get_job_status("invalid_job_id") is None
