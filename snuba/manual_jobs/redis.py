import uuid
from datetime import datetime
from typing import List, Optional, Sequence

from snuba.manual_jobs.job_status import JobStatus
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.serializable_exception import SerializableException

_redis_client = get_redis_client(RedisClientKey.MANUAL_JOBS)

# Lua script for atomic check-and-delete lock release
# This ensures only the lock owner can release the lock
_RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


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


def _acquire_job_lock(job_id: str) -> Optional[str]:
    """
    Acquire a lock for a job.

    Returns a unique lock token if the lock was acquired, or None if the lock
    is already held. The token must be passed to _release_job_lock to release
    the lock, ensuring only the lock owner can release it.
    """
    token = str(uuid.uuid4())
    if _redis_client.set(
        name=_build_job_lock_key(job_id), value=token, nx=True, ex=(24 * 60 * 60)
    ):
        return token
    return None


def _push_job_log_line(job_id: str, line: str) -> bool:
    return bool(_redis_client.rpush(_build_job_log_key(job_id), line))


def _release_job_lock(job_id: str, token: Optional[str] = None) -> bool:
    """
    Release a job lock.

    If a token is provided, the lock is only released if the token matches
    the current lock value (atomic check-and-delete). This prevents releasing
    a lock that was acquired by another process after the original lock expired.

    If no token is provided, the lock is unconditionally deleted (legacy behavior).

    Returns True if the lock was released, False otherwise.
    """
    if token is None:
        # Legacy behavior for backward compatibility
        _redis_client.delete(_build_job_lock_key(job_id))
        return True

    # Use Lua script for atomic check-and-delete
    result = _redis_client.eval(
        _RELEASE_LOCK_SCRIPT, 1, _build_job_lock_key(job_id), token
    )
    return bool(result)


def _record_start_time(job_id: str) -> None:
    _redis_client.set(name=_build_start_time_key(job_id), value=datetime.utcnow().isoformat())


def _set_job_status(job_id: str, status: JobStatus) -> JobStatus:
    if not _redis_client.set(name=_build_job_status_key(job_id), value=status.value):
        raise SerializableException(f"Failed to set job status {status} on {job_id}")
    return status


def _set_job_type(job_id: str, job_type: str) -> None:
    _redis_client.set(name=_build_job_type_key(job_id), value=job_type)


def _get_job_type(job_id: str) -> str:
    """
    Get the job type for a given job ID.

    Raises SerializableException if the job type is not found.
    """
    result = _redis_client.get(name=_build_job_type_key(job_id))
    if result is None:
        raise SerializableException(f"Job type not found for job_id: {job_id}")
    return str(result.decode())


def _get_job_types_multi(job_ids_keys: Sequence[str]) -> List[Optional[str]]:
    """
    Get job types for multiple job IDs.

    Returns a list of job types, with None for any job IDs that don't have
    a job type set.
    """
    if len(job_ids_keys) == 0:
        return []

    with _redis_client.pipeline(transaction=False) as pipeline:
        for job_id_key in job_ids_keys:
            pipeline.get(job_id_key)
        redis_statuses = pipeline.execute()

    return [
        job_type.decode() if job_type is not None else None
        for job_type in redis_statuses
    ]


def _get_job_status_multi(job_ids_keys: Sequence[str]) -> List[JobStatus]:
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
