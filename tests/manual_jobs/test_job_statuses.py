from unittest.mock import patch

import pytest

from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import get_job_status, run_job

JOB_ID = "abc1234"
async_job_spec = JobSpec(job_id=JOB_ID, job_type="AsyncJob", is_async=True)


class AsyncJob(Job):
    def __init__(self, job_spec: JobSpec):
        super().__init__(job_spec)

    def execute(self, logger: JobLogger) -> None:
        pass

    @classmethod
    def async_finished_check(cls, job_id: str) -> bool:
        return False


@pytest.mark.redis_db
def test_async_job_status_changes_correctly() -> None:
    assert get_job_status(JOB_ID) == JobStatus.NOT_STARTED
    run_job(async_job_spec)
    assert get_job_status(JOB_ID) == JobStatus.ASYNC_RUNNING_BACKGROUND
    with patch.object(AsyncJob, "async_finished_check", return_value=True):
        assert get_job_status(JOB_ID) == JobStatus.FINISHED
