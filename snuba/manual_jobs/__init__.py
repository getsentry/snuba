import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, MutableMapping, Optional, cast

from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory

logger = logging.getLogger("snuba.manual_jobs")


class JobStatus(StrEnum):
    RUNNING = "running"
    FINISHED = "finished"
    NOT_STARTED = "not_started"
    FAILED = "failed"


@dataclass
class JobSpec:
    job_id: str
    job_type: str
    params: Optional[MutableMapping[Any, Any]] = None


class Job(ABC, metaclass=RegisteredClass):
    def __init__(self, job_spec: JobSpec) -> None:
        self.job_spec = job_spec
        if job_spec.params:
            for k, v in job_spec.params.items():
                setattr(self, k, v)

    @abstractmethod
    def execute(self, logger: logging.Logger) -> None:
        raise NotImplementedError

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "Job":
        return cast("Job", cls.class_from_name(name))


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.manual_jobs"
)
