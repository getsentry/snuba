import pytest

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.redis import (
    MANUAL_JOB_LOG_MAX_LINES,
    MANUAL_JOB_STATE_TTL_SECONDS,
    _build_job_log_key,
    _build_job_status_key,
    _build_job_type_key,
    _build_start_time_key,
    _push_job_log_line,
    _redis_client,
)
from snuba.manual_jobs.runner import run_job, view_job_logs

JOB_ID = "abc1234"
job_spec = JobSpec(job_id=JOB_ID, job_type="ToyJob")


@pytest.mark.redis_db
def test_job_state_keys_expire() -> None:
    """Every key a job run leaves behind must expire. Without this the four keys
    per job id stay in Redis forever on a long-lived cluster."""
    run_job(job_spec)

    for build_key in (
        _build_start_time_key,
        _build_job_status_key,
        _build_job_log_key,
        _build_job_type_key,
    ):
        key = build_key(JOB_ID)
        assert _redis_client.exists(key) == 1, f"{key} was not written"
        ttl = _redis_client.ttl(key)
        assert ttl > 0, f"{key} has no expiry"
        # Check the value, not only that some expiry is set, so that a wrong
        # unit does not pass.
        assert MANUAL_JOB_STATE_TTL_SECONDS - 60 < ttl <= MANUAL_JOB_STATE_TTL_SECONDS, (
            f"{key} has an unexpected expiry {ttl}"
        )


@pytest.mark.redis_db
def test_job_log_expiry_is_refreshed_on_each_line() -> None:
    """A job that runs longer than the retention window must not lose its own log
    while it is still writing to it, so every line resets the expiry."""
    log_key = _build_job_log_key(JOB_ID)
    _push_job_log_line(JOB_ID, "first line")
    _redis_client.expire(log_key, 60)
    assert _redis_client.ttl(log_key) <= 60

    _push_job_log_line(JOB_ID, "second line")
    assert _redis_client.ttl(log_key) > 60


@pytest.mark.redis_db
def test_job_log_is_capped_to_the_newest_lines() -> None:
    """The log list is capped so that a chatty job cannot grow it without bound.
    The newest lines are kept, because a failing run appends its error and
    traceback last."""
    for i in range(MANUAL_JOB_LOG_MAX_LINES + 50):
        _push_job_log_line(JOB_ID, f"line {i}")

    assert _redis_client.llen(_build_job_log_key(JOB_ID)) == MANUAL_JOB_LOG_MAX_LINES

    # A capped log is still short enough for the admin UI and the CLI to display.
    logs = view_job_logs(JOB_ID)
    assert len(logs) == MANUAL_JOB_LOG_MAX_LINES
    assert logs[0] == "line 50"
    assert logs[-1] == f"line {MANUAL_JOB_LOG_MAX_LINES + 49}"
