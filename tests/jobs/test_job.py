from unittest.mock import patch

import pytest

from snuba.manual_jobs import Job, JobSpec
from snuba.manual_jobs.job_loader import _JobLoader
from snuba.manual_jobs.runner import JobStatus, get_job_status, run_job
from snuba.utils.serializable_exception import SerializableException

JOB_ID = "abc1234"
test_job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


class FailJob(Job):
    def __init__(self):
        pass

    def execute(self):
        raise SerializableException("Intended failure")


@pytest.mark.redis_db
def test_job_status_changes_to_finished() -> None:
    assert get_job_status(JOB_ID) is None
    run_job(test_job_spec, False)
    assert get_job_status(JOB_ID) == JobStatus.FINISHED


@pytest.mark.redis_db
def test_job_with_exception_causes_failure() -> None:
    with patch.object(_JobLoader, "get_job_instance") as MockGetInstance:
        MockGetInstance.return_value = FailJob()
        assert get_job_status(JOB_ID) is None
        run_job(test_job_spec, False)
        assert get_job_status(JOB_ID) == JobStatus.FAILED


@pytest.mark.redis_db
def test_job_status_with_invalid_job_id() -> None:
    assert get_job_status("invalid_job_id") is None
