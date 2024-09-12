import os
from abc import ABC, abstractmethod
from typing import Any, cast

from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class Job(ABC, metaclass=RegisteredClass):
    def __init__(self, dry_run: bool, **kwargs: Any) -> None:
        self.dry_run = dry_run
        for k, v in kwargs.items():
            setattr(self, k, v)

    @abstractmethod
    def execute(self) -> None:
        pass

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "Job":
        return cast("Job", cls.class_from_name(name))


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.manual_jobs"
)
