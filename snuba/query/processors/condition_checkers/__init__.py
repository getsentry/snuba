from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Type, cast

from snuba.clickhouse.query import Expression
from snuba.query.conditions import ConditionFunctions, condition_pattern
from snuba.query.matchers import Any
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Or, Param, Pattern, String
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


EQ_CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ},
    ColumnPattern(None, Param("lhs", Any(str))),
    LiteralPattern(Any(int)),
    commutative=True,
)

FULL_CONDITION_PATTERN = Or(
    [
        EQ_CONDITION_PATTERN,
        FunctionCallPattern(
            String(ConditionFunctions.IN),
            (
                ColumnPattern(None, Param("lhs", Any(str))),
                FunctionCallPattern(Or([String("tuple"), String("array")]), None),
            ),
        ),
    ],
)


def _check_expression(
    pattern: Pattern[Expression], expression: Expression, column_name: str
) -> bool:
    match = pattern.match(expression)
    return match is not None and match.optional_string("lhs") == column_name


class ProjectIdEnforcer(ConditionChecker):
    def get_id(self) -> str:
        return "project_id"

    def check(self, expression: Expression) -> bool:
        return _check_expression(FULL_CONDITION_PATTERN, expression, "project_id")


class OrgIdEnforcer(ConditionChecker):
    def __init__(self, field_name: str = "org_id") -> None:
        self.field_name = field_name

    def get_id(self) -> str:
        return self.field_name

    def check(self, expression: Expression) -> bool:
        return _check_expression(EQ_CONDITION_PATTERN, expression, self.field_name)


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.query.processors.condition_checkers",
)
