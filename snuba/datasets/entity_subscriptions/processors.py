from abc import abstractmethod
from typing import Any, Mapping, Optional, Type, Union, cast

from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.utils.registered_class import RegisteredClass


class EntitySubscriptionProcessor(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["EntitySubscriptionProcessor"]:
        return cast(Type["EntitySubscriptionProcessor"], cls.class_from_name(name))

    @abstractmethod
    def to_dict(self, metadata: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def process(
        self,
        query: Union[CompositeQuery[Entity], Query],
        metadata: Mapping[str, Any],
        offset: Optional[int] = None,
    ) -> None:
        raise NotImplementedError


class AddColumnCondition(EntitySubscriptionProcessor):
    def __init__(self, extra_condition_data_key: str, extra_condition_column: str):
        self.extra_condition_data_key = extra_condition_data_key
        self.extra_condition_column = extra_condition_column

    def to_dict(self, metadata: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.extra_condition_data_key not in metadata:
            raise InvalidQueryException(
                f"{self.extra_condition_data_key} not found in metadata: {metadata}"
            )
        return {self.extra_condition_data_key: metadata[self.extra_condition_data_key]}

    def process(
        self,
        query: Union[CompositeQuery[Entity], Query],
        metadata: Mapping[str, Any],
        offset: Optional[int] = None,
    ) -> None:
        if self.extra_condition_data_key not in metadata:
            raise InvalidQueryException(
                f"'{self.extra_condition_data_key}' not found in metadata: {metadata}"
            )

        condition_to_add: Expression = binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, self.extra_condition_column),
            Literal(None, metadata[self.extra_condition_data_key]),
        )
        condition = query.get_condition()
        if condition:
            new_condition = binary_condition(BooleanFunctions.AND, condition, condition_to_add)
            query.set_ast_condition(new_condition)
        else:
            query.set_ast_condition(condition_to_add)
