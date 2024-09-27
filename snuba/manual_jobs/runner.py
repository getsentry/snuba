import os
from enum import Enum
from typing import Any, Mapping, Optional

import simplejson
from sentry_sdk import capture_exception

from snuba.manual_jobs import JobSpec
from snuba.manual_jobs.job_loader import _JobLoader
from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.serializable_exception import SerializableException

redis_client = get_redis_client(RedisClientKey.JOB)


class JobStatus(Enum):
    RUNNING = "running"
    FINISHED = "finished"
    NOT_STARTED = "not_started"
    FAILED = "failed"


MANIFEST_FILENAME = "job_manifest.json"


def _read_manifest_from_path(filename: str) -> Mapping[str, JobSpec]:
    local_root = os.path.dirname(__file__)

    with open(os.path.join(local_root, filename)) as stream:
        contents = simplejson.loads(stream.read())

        job_specs = {}
        for content in contents:
            job_spec = _build_job_spec_from_entry(content)
            job_specs[job_spec.job_id] = job_spec
        return job_specs


def _build_job_spec_from_entry(content: Any) -> JobSpec:
    job_id = content["id"]
    assert isinstance(job_id, str)
    job_type = content["job_type"]
    assert isinstance(job_type, str)

    job_spec = JobSpec(
        job_id=job_id,
        job_type=job_type,
        params=content.get("params"),
    )

    return job_spec


def _job_lock_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:lock"


def _job_status_key(job_id: str) -> str:
    return f"snuba:manual_jobs:{job_id}:execution_status"


def _acquire_job_lock(job_id: str) -> bool:
    return redis_client.set(
        name=_job_lock_key(job_id), value=1, nx=True, ex=(24 * 60 * 60)
    )


def _release_job_lock(job_id: str) -> bool:
    redis_client.delete(_job_lock_key(job_id))


def _set_job_status(job_id: str, status: JobStatus) -> bool:
    return redis_client.set(name=_job_status_key(job_id), value=status.name)


def get_job_status(job_id: str) -> Optional[JobStatus]:
    return redis_client.get(name=_job_status_key(job_id))


def list_job_specs(
    manifest_filename: str = MANIFEST_FILENAME,
) -> Mapping[str, JobSpec]:
    return _read_manifest_from_path(manifest_filename)


def run_job(job_spec: JobSpec, dry_run: bool):
    current_job_status = get_job_status(job_spec.job_id)
    if current_job_status is not None and current_job_status != JobStatus.NOT_STARTED:
        raise SerializableException(
            f"attempting to run job that has been started, status = {current_job_status}"
        )

    have_lock = _acquire_job_lock(job_spec.job_id)
    if not have_lock:
        raise SerializableException("could not acquire lock, job already running")

    _set_job_status(job_spec.job_id, JobStatus.NOT_STARTED)

    job_to_run = _JobLoader.get_job_instance(job_spec, dry_run)

    try:
        _set_job_status(job_spec.job_id, JobStatus.RUNNING)
        job_to_run.execute()
        _set_job_status(job_spec.job_id, JobStatus.FINISHED)
    except BaseException:
        _set_job_status(job_spec.job_id, JobStatus.FAILED)
        capture_exception()
    finally:
        _release_job_lock(job_spec.job_id)
