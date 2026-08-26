import typing
from collections.abc import Sequence
from datetime import datetime

from snuba.manual_jobs.job_status import JobStatus
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.serializable_exception import SerializableException

_redis_client = get_redis_client(RedisClientKey.MANUAL_JOBS)

# One run of a job may hold the lock for this long
MANUAL_JOB_LOCK_TTL_SECONDS = 24 * 60 * 60

# How long the record of a job run stays in Redis -
# past 90 days the rows a job touched have aged out anyway.
MANUAL_JOB_STATE_TTL_SECONDS = 90 * 24 * 60 * 60

# Longest log the runner keeps for one job
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
    # Keep the job type alive at least as long as the status
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
