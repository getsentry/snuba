import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.redis import _acquire_job_lock
from snuba.manual_jobs.runner import JobLockedException, run_job

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


@pytest.mark.redis_db
def test_job_lock() -> None:
    _acquire_job_lock(JOB_ID)
    with pytest.raises(JobLockedException):
        run_job(test_job_spec)
