from threading import Thread
from time import sleep
from unittest.mock import patch

import pytest

from snuba.manual_jobs import Job, JobSpec
from snuba.manual_jobs.job_loader import _JobLoader
from snuba.manual_jobs.runner import (
    JobLockedException,
    JobStatus,
    JobStatusException,
    _acquire_job_lock,
    get_job_status,
    run_job,
)
from snuba.utils.serializable_exception import SerializableException

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


class FailJob(Job):
    def __init__(self) -> None:
        pass

    def execute(self) -> None:
        raise SerializableException("Intended failure")


class SlowJob(Job):
    def __init__(self) -> None:
        self.stop = False

    def execute(self) -> None:
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


@pytest.mark.redis_db
def test_job_lock() -> None:
    _acquire_job_lock(JOB_ID)
    with pytest.raises(JobLockedException):
        run_job(test_job_spec)
