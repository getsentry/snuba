import logging
import multiprocessing
import os
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, MutableMapping, Optional, cast

from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory

logger = logging.getLogger("snuba.manual_jobs")


class JobStatus(StrEnum):
    RUNNING = "running"
    ASYNC_RUNNING_BACKGROUND = "async_running_background"
    FINISHED = "finished"
    NOT_STARTED = "not_started"
    FAILED = "failed"


class JobLogger(ABC):
    @abstractmethod
    def debug(self, line: str) -> None:
        pass

    @abstractmethod
    def info(self, line: str) -> None:
        pass

    @abstractmethod
    def warning(self, line: str) -> None:
        pass

    @abstractmethod
    def warn(self, line: str) -> None:
        pass

    @abstractmethod
    def error(self, line: str) -> None:
        pass


@dataclass
class JobSpec:
    job_id: str
    job_type: str
    is_async: Optional[bool] = False
    params: Optional[MutableMapping[Any, Any]] = None


class Job(ABC, metaclass=RegisteredClass):
    def __init__(self, job_spec: JobSpec) -> None:
        self.job_spec = job_spec
        if job_spec.params:
            for k, v in job_spec.params.items():
                setattr(self, k, v)

    @abstractmethod
    def _execute(self, logger: JobLogger) -> None:
        raise NotImplementedError

    def execute(self, logger: JobLogger, async_job_statuses: Any = None) -> None:
        if self.job_spec.is_async:
            assert async_job_statuses is not None, "Please pass the async_job_status in"

        self._execute(logger)

        if self.job_spec.is_async:
            async_job_statuses[self.job_spec.job_id] = 1

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "Job":
        return cast("Job", cls.class_from_name(name))


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.manual_jobs"
)
