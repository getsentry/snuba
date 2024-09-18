import logging
import os
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional, cast

from snuba.redis import RedisClientKey, get_redis_client
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory

logger = logging.getLogger("snuba_init")
redis_client = get_redis_client(RedisClientKey.JOB)
job_status_hash = "snuba-job-status-hash"

RUNNING = "running"
FINISHED = "finished"
NOT_STARTED = "not started"


@dataclass
class JobSpec:
    job_id: str
    job_type: str
    params: Optional[MutableMapping[Any, Any]] = None


class Job(ABC, metaclass=RegisteredClass):
    def __init__(self, job_spec: JobSpec, dry_run: bool) -> None:
        self.job_spec = job_spec
        self.dry_run = dry_run
        if job_spec.params:
            for k, v in job_spec.params.items():
                setattr(self, k, v)
        self._set_status(NOT_STARTED)

    def execute(self) -> None:
        self._set_status(RUNNING)
        self._execute()
        self._set_status(FINISHED)

    @abstractmethod
    def _execute(self) -> None:
        pass

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "Job":
        return cast("Job", cls.class_from_name(name))

    def _set_status(self, status: str) -> None:
        redis_client.hset(job_status_hash, self.job_spec.job_id, status)


def get_job_status(job_id: str) -> str:
    status = redis_client.hget(job_status_hash, job_id)
    if status is None:
        raise KeyError
    else:
        return typing.cast(str, status.decode("utf-8"))


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.manual_jobs"
)
