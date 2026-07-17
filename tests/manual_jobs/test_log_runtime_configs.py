import json
from collections.abc import Sequence
from typing import Any, cast

import pytest

from snuba import state
from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_logging import get_console_job_logger
from snuba.manual_jobs.log_runtime_configs import (
    PAYLOAD_END_MARKER,
    PAYLOAD_START_MARKER,
    LogRuntimeConfigs,
)
from snuba.redis import RedisClientKey, get_redis_client


def _extract_payload(logs: Sequence[str]) -> dict[str, Any]:
    start = next(i for i, line in enumerate(logs) if PAYLOAD_START_MARKER in line)
    end = next(i for i, line in enumerate(logs) if PAYLOAD_END_MARKER in line)
    body = "\n".join(logs[start + 1 : end])
    body = body[body.index("{") :]
    return cast(dict[str, Any], json.loads(body))


def _run_and_get_payload() -> dict[str, Any]:
    logs: list[str] = []

    class _CapturingLogger:
        def debug(self, line: str) -> None:
            logs.append(line)

        def info(self, line: str) -> None:
            logs.append(line)

        def warning(self, line: str) -> None:
            logs.append(line)

        def warn(self, line: str) -> None:
            logs.append(line)

        def error(self, line: str) -> None:
            logs.append(line)

    job = LogRuntimeConfigs(JobSpec(job_id="log_runtime_configs", job_type="LogRuntimeConfigs"))
    job.execute(cast(Any, _CapturingLogger()))
    return _extract_payload(logs)


@pytest.mark.redis_db
def test_dumps_runtime_configs_from_config_client() -> None:
    state.set_config("a_test_config", 42)
    try:
        payload = _run_and_get_payload()
    finally:
        state.delete_config("a_test_config")

    # Values are dumped raw (as stored in Redis), grouped by config key.
    assert payload["config"]["snuba-config"]["a_test_config"] == "42"


@pytest.mark.redis_db
def test_only_reads_known_config_keys() -> None:
    # A blind scan of the (shared) config client would leak unrelated keys.
    # Set such a key and confirm it is never dumped.
    client = get_redis_client(RedisClientKey.CONFIG)
    client.set("roles-someone@example.com", "admin")
    try:
        payload = _run_and_get_payload()
    finally:
        client.delete("roles-someone@example.com")

    allowed = {
        "snuba-config",
        "snuba-config-description",
        "snuba-config-changes",
        "capman",
        "cbrs",
    }
    assert set(payload["config"].keys()) <= allowed
    assert "roles-someone@example.com" not in payload["config"]


@pytest.mark.redis_db
def test_repeatable_direct_execution(capsys: pytest.CaptureFixture[str]) -> None:
    # Executing directly (as the `snuba jobs dump_runtime_configs` CLI does)
    # bypasses the job-status guard, so it can be run any number of times.
    job = LogRuntimeConfigs(JobSpec(job_id="log_runtime_configs", job_type="LogRuntimeConfigs"))
    for _ in range(3):
        job.execute(get_console_job_logger())

    out = capsys.readouterr().out
    assert out.count(PAYLOAD_START_MARKER) == 3
    assert out.count(PAYLOAD_END_MARKER) == 3
