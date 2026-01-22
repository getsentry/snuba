import logging
import os
import traceback
from typing import Any, Mapping, Sequence, Union

import simplejson

from snuba.manual_jobs import Job, JobSpec
from snuba.manual_jobs.job_loader import _JobLoader
from snuba.manual_jobs.job_logging import get_job_logger
from snuba.manual_jobs.job_status import JobStatus
from snuba.manual_jobs.redis import (
    _acquire_job_lock,
    _build_job_log_key,
    _build_job_status_key,
    _get_job_status_multi,
    _get_job_type,
    _record_start_time,
    _redis_client,
    _release_job_lock,
    _set_job_status,
)
from snuba.utils.serializable_exception import SerializableException

logger = logging.getLogger("snuba.manual_jobs")


class JobLockedException(SerializableException):
    def __init__(self, job_id: str):
        super().__init__(f"Job {job_id} lock exists, not available to run")


class JobStatusException(SerializableException):
    def __init__(self, job_id: str, status: JobStatus):
        super().__init__(f"Job {job_id} has run before, status = {status}, not available to run")


MANIFEST_FILENAME = "job_manifest.json"


def _read_manifest_from_path(filename: str) -> Mapping[str, JobSpec]:
    local_root = os.path.join(os.path.dirname(__file__), "..", "env")

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


def _update_job_status_if_async(job_id: str, job_status: JobStatus) -> JobStatus:
    if job_status is JobStatus.ASYNC_RUNNING_BACKGROUND and Job.get_from_name(
        _get_job_type(job_id)
    ).async_finished_check(job_id):
        job_status = JobStatus.FINISHED
        _set_job_status(job_id, JobStatus.FINISHED)
    return job_status


def get_job_status(job_id: str) -> JobStatus:
    redis_status = _redis_client.get(name=_build_job_status_key(job_id))
    status = _update_job_status_if_async(
        job_id,
        JobStatus(redis_status.decode("utf-8")) if redis_status else JobStatus.NOT_STARTED,
    )
    return status


def list_job_specs(
    manifest_filename: str = MANIFEST_FILENAME,
) -> Mapping[str, JobSpec]:
    return _read_manifest_from_path(manifest_filename)


def list_job_specs_with_status(
    manifest_filename: str = MANIFEST_FILENAME,
) -> Mapping[str, Mapping[str, Union[JobSpec, JobStatus]]]:
    specs = list_job_specs(manifest_filename)
    job_ids = list(specs.keys())
    statuses = _get_job_status_multi([_build_job_status_key(job_id) for job_id in job_ids])
    for i in range(len(job_ids)):
        statuses[i] = _update_job_status_if_async(job_ids[i], statuses[i])
    return {
        job_id: {"spec": specs[job_id], "status": statuses[i]} for i, job_id in enumerate(job_ids)
    }


def run_job(job_spec: JobSpec) -> JobStatus:
    current_job_status = get_job_status(job_spec.job_id)
    job_logger = get_job_logger(logger, job_spec.job_id)
    if current_job_status is not None and current_job_status != JobStatus.NOT_STARTED:
        raise JobStatusException(job_id=job_spec.job_id, status=current_job_status)

    lock_token = _acquire_job_lock(job_spec.job_id)
    if lock_token is None:
        raise JobLockedException(job_spec.job_id)

    current_job_status = _set_job_status(job_spec.job_id, JobStatus.NOT_STARTED)

    job_to_run = _JobLoader.get_job_instance(job_spec)

    running_status_type = (
        JobStatus.ASYNC_RUNNING_BACKGROUND if job_spec.is_async else JobStatus.RUNNING
    )
    try:
        current_job_status = _set_job_status(job_spec.job_id, running_status_type)
        _record_start_time(job_spec.job_id)
        job_to_run.execute(job_logger)
        if not job_spec.is_async:
            current_job_status = _set_job_status(job_spec.job_id, JobStatus.FINISHED)
            job_logger.info("[runner] job execution finished")
    except BaseException as e:
        current_job_status = _set_job_status(job_spec.job_id, JobStatus.FAILED)
        job_logger.error(f"[runner] job execution failed {e}")
        job_logger.info(f"[runner] exception {traceback.format_exc()}")
        raise e
    finally:
        _release_job_lock(job_spec.job_id, lock_token)

    return current_job_status


def view_job_logs(job_id: str) -> Sequence[str]:
    job_logs_length = _redis_client.llen(name=_build_job_log_key(job_id))
    if job_logs_length == 0:
        return []
    assert job_logs_length < 500, "Job logs are too long to display"
    job_logs = _redis_client.lrange(
        name=_build_job_log_key(job_id), start=0, end=job_logs_length - 1
    )
    return [log.decode("utf-8") for log in job_logs]
