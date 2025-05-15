from unittest.mock import patch

import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.redis import _build_start_time_key, _redis_client
from snuba.manual_jobs.runner import run_job

JOB_ID = "abc1234"
job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


@pytest.mark.redis_db
def test_record_job_start_time_correctly() -> None:
    with patch("snuba.manual_jobs.redis.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value.isoformat.return_value = (
            "2024-10-23T01:12:23.456789"
        )
        run_job(job_spec)
        assert (
            _redis_client.get(name=_build_start_time_key(JOB_ID)).decode()
            == "2024-10-23T01:12:23.456789"
        )
