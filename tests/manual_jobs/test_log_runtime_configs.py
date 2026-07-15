from collections.abc import Sequence

import pytest

from snuba import state
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.runner import run_job, view_job_logs

JOB_ID = "log_runtime_configs_test"


def _make_job_spec(**params: object) -> JobSpec:
    return JobSpec(
        job_id=JOB_ID,
        job_type="LogRuntimeConfigs",
        params=params or None,
    )


def _log_contains(logs: Sequence[str], substring: str) -> bool:
    return any(substring in line for line in logs)


@pytest.mark.redis_db
def test_logs_runtime_configs() -> None:
    state.set_config("a_test_config", 42)
    try:
        assert run_job(_make_job_spec()) == JobStatus.FINISHED
    finally:
        state.delete_config("a_test_config")

    logs = view_job_logs(JOB_ID)
    assert _log_contains(logs, "runtime configs (snuba.state)")
    assert _log_contains(logs, "runtime_config a_test_config = 42")
    assert _log_contains(logs, "allocation policy configs")
    assert _log_contains(logs, "CBRS")
    assert _log_contains(logs, "done logging all runtime configs")


@pytest.mark.redis_db
def test_logs_allocation_policy_configs() -> None:
    assert run_job(_make_job_spec()) == JobStatus.FINISHED

    logs = view_job_logs(JOB_ID)
    assert _log_contains(logs, "allocation_policy")
    assert _log_contains(logs, "is_enforced")
