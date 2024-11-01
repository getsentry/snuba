import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional, cast

from snuba.manual_jobs.redis import _set_job_type
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory

logger = logging.getLogger("snuba.manual_jobs")


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
        self.is_async = job_spec.is_async
        _set_job_type(job_spec.job_id, job_spec.job_type)
        if job_spec.params:
            for k, v in job_spec.params.items():
                setattr(self, k, v)

    @abstractmethod
    def execute(self, logger: JobLogger) -> None:
        raise NotImplementedError

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "Job":
        return cast("Job", cls.class_from_name(name))

    @classmethod
    def async_finished_check(cls, job_id: str) -> bool:
        raise NotImplementedError


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.manual_jobs"
)
