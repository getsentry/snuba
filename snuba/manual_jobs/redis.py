import typing
from datetime import datetime
from typing import List, Sequence

from snuba.manual_jobs.job_status import JobStatus
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.serializable_exception import SerializableException

_redis_client = get_redis_client(RedisClientKey.MANUAL_JOBS)


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
        _redis_client.set(name=_build_job_lock_key(job_id), value=1, nx=True, ex=(24 * 60 * 60))
    )


def _push_job_log_line(job_id: str, line: str) -> bool:
    return bool(_redis_client.rpush(_build_job_log_key(job_id), line))


def _release_job_lock(job_id: str) -> None:
    _redis_client.delete(_build_job_lock_key(job_id))


def _record_start_time(job_id: str) -> None:
    _redis_client.set(name=_build_start_time_key(job_id), value=datetime.utcnow().isoformat())


def _set_job_status(job_id: str, status: JobStatus) -> JobStatus:
    if not _redis_client.set(name=_build_job_status_key(job_id), value=status.value):
        raise SerializableException(f"Failed to set job status {status} on {job_id}")
    return status


def _set_job_type(job_id: str, job_type: str) -> None:
    _redis_client.set(name=_build_job_type_key(job_id), value=job_type)


def _get_job_type(job_id: str) -> str:
    return typing.cast(str, _redis_client.get(name=_build_job_type_key(job_id)).decode())


def _get_job_types_multi(job_ids_keys: Sequence[str]) -> List[str]:
    with _redis_client.pipeline(transaction=False) as pipeline:
        for job_id_key in job_ids_keys:
            pipeline.get(job_id_key)
        redis_statuses = pipeline.execute()

    return [job_type.decode() for job_type in redis_statuses]


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
