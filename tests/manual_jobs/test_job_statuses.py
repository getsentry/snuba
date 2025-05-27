from threading import Thread
from time import sleep
from unittest.mock import patch

import pytest

from snuba.manual_jobs import Job, JobLogger, JobSpec
from snuba.manual_jobs.job_loader import _JobLoader
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import JobStatusException, get_job_status, run_job
from snuba.utils.serializable_exception import SerializableException

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


class FailJob(Job):
    def __init__(self) -> None:
        pass

    def execute(self, logger: JobLogger) -> None:
        raise SerializableException("Intended failure")


class SlowJob(Job):
    def __init__(self) -> None:
        self.stop = False

    def execute(self, logger: JobLogger) -> None:
        while not self.stop:
            sleep(0.005)


@pytest.mark.redis_db
def test_job_status_changes_to_finished() -> None:
    assert get_job_status(JOB_ID) == JobStatus.NOT_STARTED
    run_job(test_job_spec)
    assert get_job_status(JOB_ID) == JobStatus.FINISHED
    with pytest.raises(JobStatusException):
        run_job(test_job_spec)


@pytest.mark.redis_db
def test_job_with_exception_causes_failure() -> None:
    with patch.object(_JobLoader, "get_job_instance") as MockGetInstance:
        MockGetInstance.return_value = FailJob()
        assert get_job_status(JOB_ID) == JobStatus.NOT_STARTED
        with pytest.raises(SerializableException):
            run_job(test_job_spec)
        assert get_job_status(JOB_ID) == JobStatus.FAILED


@pytest.mark.redis_db
def test_slow_job_stay_running() -> None:
    with patch.object(_JobLoader, "get_job_instance") as MockGetInstance:
        job = SlowJob()
        MockGetInstance.return_value = job
        assert get_job_status(JOB_ID) == JobStatus.NOT_STARTED
        t = Thread(target=run_job, name="slow-background-job", args=[test_job_spec])
        t.start()
        sleep(0.1)
        assert get_job_status(JOB_ID) == JobStatus.RUNNING
        job.stop = True


@pytest.mark.redis_db
def test_job_status_with_invalid_job_id() -> None:
    assert get_job_status("invalid_job_id") == JobStatus.NOT_STARTED


ASYNC_JOB_ID = "abc1234"
async_job_spec = JobSpec(job_id=ASYNC_JOB_ID, job_type="AsyncJob", is_async=True)


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
    assert get_job_status(ASYNC_JOB_ID) == JobStatus.NOT_STARTED
    run_job(async_job_spec)
    assert get_job_status(ASYNC_JOB_ID) == JobStatus.ASYNC_RUNNING_BACKGROUND
    with patch.object(AsyncJob, "async_finished_check", return_value=True):
        assert get_job_status(ASYNC_JOB_ID) == JobStatus.FINISHED
