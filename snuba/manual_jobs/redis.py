import typing
from collections.abc import Sequence
from datetime import datetime

from snuba.manual_jobs.job_status import JobStatus
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.serializable_exception import SerializableException

_redis_client = get_redis_client(RedisClientKey.MANUAL_JOBS)

# One run of a job may hold the lock for this long. This also sets the longest
# run the rest of this module has to plan for.
MANUAL_JOB_LOCK_TTL_SECONDS = 24 * 60 * 60

# How long the record of a job run stays in Redis. All four state keys
# (start time, execution status, log, job type) share this one window.
#
# The record is a human-facing history, not operational state. `run_job` refuses
# to start a manifest job whose status key exists, and the admin UI replaces the
# Run button with View Logs for the same reason. So when the record goes, a job
# that already ran reports NOT_STARTED and looks re-runnable to the next engineer.
# The window has to cover a realistic re-run window, not be as short as possible.
#
# 90 days is the longest standard retention Snuba allows for the ClickHouse data
# that the destructive jobs act on (deletes, scrubs, mutations). The live policy in
# `snuba/state/retention.py` caps standard retention at 90 days. Long-term
# downsampled storage runs longer, but the row-touching jobs write only to tier-1
# tables
# (`errors_local`, `eap_spans_2_local`). Past 90 days the rows a job touched have
# aged out anyway, so "did we already run this?" no longer has a target. The value
# is its own constant on purpose: a region that shortens its data retention should
# not silently shorten its operator history. The window is also far longer than one
# run, which MANUAL_JOB_LOCK_TTL_SECONDS plans for at 24 hours.
MANUAL_JOB_STATE_TTL_SECONDS = 90 * 24 * 60 * 60

# Longest log the runner keeps for one job. The admin UI and the
# `snuba jobs view-logs` CLI render the whole list in one response, and
# `view_job_logs` refuses to show more lines than this. The cap counts lines, not
# bytes, and a job that logs full SQL statements can still build a list of a few
# megabytes. The list keeps the newest lines: on a failure the runner appends the
# error and the traceback last, so the tail is the part an operator needs.
MANUAL_JOB_LOG_MAX_LINES = 500


def _build_job_lock_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:lock"


def _build_start_time_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:start_time"


def _build_job_status_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:execution_status"


def _build_job_log_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:log"


def _build_job_type_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:job_type"


def _acquire_job_lock(job_id: str) -> bool:
    return bool(
        _redis_client.set(
            name=_build_job_lock_key(job_id), value=1, nx=True, ex=MANUAL_JOB_LOCK_TTL_SECONDS
        )
    )


def _push_job_log_line(job_id: str, line: str) -> bool:
    key = _build_job_log_key(job_id)
    # All three commands act on one key, so this pipeline stays on one slot in a
    # Redis cluster. The expiry is refreshed on every line, so the retention window
    # of a long job starts at its last log line and not at its first.
    with _redis_client.pipeline(transaction=False) as pipeline:
        pipeline.rpush(key, line)
        pipeline.ltrim(key, -MANUAL_JOB_LOG_MAX_LINES, -1)
        pipeline.expire(key, MANUAL_JOB_STATE_TTL_SECONDS)
        results = pipeline.execute()
    return bool(results[0])


def _release_job_lock(job_id: str) -> None:
    _redis_client.delete(_build_job_lock_key(job_id))


def _record_start_time(job_id: str) -> None:
    _redis_client.set(
        name=_build_start_time_key(job_id),
        value=datetime.utcnow().isoformat(),
        ex=MANUAL_JOB_STATE_TTL_SECONDS,
    )


def _set_job_status(job_id: str, status: JobStatus) -> JobStatus:
    if not _redis_client.set(
        name=_build_job_status_key(job_id), value=status.value, ex=MANUAL_JOB_STATE_TTL_SECONDS
    ):
        raise SerializableException(f"Failed to set job status {status} on {job_id}")
    # Keep the job type alive at least as long as the status. `get_job_status`
    # reads the job type when the status says an async job is still running, and
    # `_get_job_type` does not handle a missing key. No job in the tree is async
    # today, so this refresh is a guard and not a fix for a live failure. The call
    # is a no-op on the first status write, which runs before the type key exists.
    _redis_client.expire(_build_job_type_key(job_id), MANUAL_JOB_STATE_TTL_SECONDS)
    return status


def _set_job_type(job_id: str, job_type: str) -> None:
    _redis_client.set(
        name=_build_job_type_key(job_id), value=job_type, ex=MANUAL_JOB_STATE_TTL_SECONDS
    )


def _get_job_type(job_id: str) -> str:
    return typing.cast(str, _redis_client.get(name=_build_job_type_key(job_id)).decode())


def _get_job_types_multi(job_ids_keys: Sequence[str]) -> list[str]:
    with _redis_client.pipeline(transaction=False) as pipeline:
        for job_id_key in job_ids_keys:
            pipeline.get(job_id_key)
        redis_statuses = pipeline.execute()

    return [job_type.decode() for job_type in redis_statuses]


def _get_job_status_multi(job_ids_keys: Sequence[str]) -> list[JobStatus]:
    if len(job_ids_keys) == 0:
        return []

    with _redis_client.pipeline(transaction=False) as pipeline:
        for job_id_key in job_ids_keys:
            pipeline.get(job_id_key)
        redis_statuses = pipeline.execute()

    return [
        redis_status.decode() if redis_status is not None else JobStatus.NOT_STARTED
        for redis_status in redis_statuses
    ]
