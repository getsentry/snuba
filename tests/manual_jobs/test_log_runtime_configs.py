import json
from collections.abc import Sequence
from typing import Any

import pytest

from snuba import state
from snuba.configs.configuration import CONFIGURABLE_COMPONENT_OVERRIDES_KEY
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.log_runtime_configs import (
    CBRS_POLICY_CLASS_NAME,
    PAYLOAD_END_MARKER,
    PAYLOAD_START_MARKER,
)
from snuba.manual_jobs.runner import run_job, view_job_logs
from snuba.query.allocation_policies import AllocationPolicy, PassthroughPolicy

JOB_ID = "log_runtime_configs_test"


def _make_job_spec() -> JobSpec:
    return JobSpec(job_id=JOB_ID, job_type="LogRuntimeConfigs")


def _extract_payload(logs: Sequence[str]) -> dict[str, Any]:
    start = next(i for i, line in enumerate(logs) if PAYLOAD_START_MARKER in line)
    end = next(i for i, line in enumerate(logs) if PAYLOAD_END_MARKER in line)
    body = "\n".join(logs[start + 1 : end])
    body = body[body.index("{") :]
    return json.loads(body)


def _find_allocation_policy() -> AllocationPolicy:
    for storage_key in get_all_storage_keys():
        for policy in get_storage(storage_key).get_allocation_policies():
            if not isinstance(policy, PassthroughPolicy):
                return policy
    raise AssertionError("no non-passthrough allocation policy found")


@pytest.mark.redis_db
def test_emits_runtime_configs_in_payload() -> None:
    state.set_config("a_test_config", 42)
    try:
        assert run_job(_make_job_spec()) == JobStatus.FINISHED
    finally:
        state.delete_config("a_test_config")

    payload = _extract_payload(view_job_logs(JOB_ID))
    assert payload["runtime_configs"]["a_test_config"] == 42


@pytest.mark.redis_db
def test_emits_component_overrides_in_payload() -> None:
    policy = _find_allocation_policy()
    policy.set_config_value("is_enforced", 0)
    expected_key = policy._build_runtime_config_key("is_enforced", {})

    assert run_job(_make_job_spec()) == JobStatus.FINISHED

    payload = _extract_payload(view_job_logs(JOB_ID))
    overrides = payload[CONFIGURABLE_COMPONENT_OVERRIDES_KEY]
    assert overrides[expected_key] == 0
    # The cbrs section is a filtered view of the same overrides.
    assert all(CBRS_POLICY_CLASS_NAME in key for key in payload["cbrs"])
    assert payload["cbrs"] == {
        key: value for key, value in overrides.items() if CBRS_POLICY_CLASS_NAME in key
    }
