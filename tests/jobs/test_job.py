import pytest

from snuba.manual_jobs import FINISHED, NOT_STARTED, JobSpec, get_job_status
from snuba.manual_jobs.toy_job import ToyJob

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")

# @pytest.fixture(autouse=True)
# def setup_test_job() -> Job:
#     return ToyJob(test_job_spec, dry_run=True)


@pytest.mark.redis_db
def test_job_status_changes_to_finished() -> None:
    # test_job = setup_test_job
    test_job = ToyJob(test_job_spec, dry_run=True)
    assert get_job_status(JOB_ID) == NOT_STARTED
    test_job.execute()
    assert get_job_status(JOB_ID) == FINISHED


@pytest.mark.redis_db
def test_job_status_with_invalid_job_id() -> None:
    assert get_job_status(JOB_ID) == NOT_STARTED
    with pytest.raises(KeyError):
        get_job_status("invalid_job_id")
