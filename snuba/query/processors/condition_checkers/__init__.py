from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Type, cast

from snuba.clickhouse.query import Expression
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class ConditionChecker(ABC, metaclass=RegisteredClass):
    """
    Checks if an expression matches a specific shape and content.

    These are declared by storages as mandatory conditions that are
    supposed to be in the query before it is executed for the query
    to be acceptable.

    This system is meant to be a failsafe mechanism to prevent
    bugs in any step of query processing to generate queries that are
    missing project_id and org_id conditions from the query.
    """

    @abstractmethod
    def get_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def check(self, expression: Expression) -> bool:
        raise NotImplementedError

    @classmethod
    def get_from_name(cls, name: str) -> Type["ConditionChecker"]:
        return cast(Type["ConditionChecker"], cls.class_from_name(name))

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> ConditionChecker:
        return cls(**kwargs)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.query.processors.condition_checkers",
)
