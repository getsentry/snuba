from abc import abstractmethod
from collections.abc import Mapping
from typing import Any, cast

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
    def get_from_name(cls, name: str) -> type["EntitySubscriptionProcessor"]:
        return cast(type["EntitySubscriptionProcessor"], cls.class_from_name(name))

    @abstractmethod
    def to_dict(self, metadata: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def process(
        self,
        query: CompositeQuery[Entity] | Query,
        metadata: Mapping[str, Any],
        offset: int | None = None,
    ) -> None:
        raise NotImplementedError


class AddColumnCondition(EntitySubscriptionProcessor):
    def __init__(
        self,
        extra_condition_data_key: str,
        extra_condition_column: str,
        fallback_data_key: str | None = None,
    ):
        self.extra_condition_data_key = extra_condition_data_key
        self.extra_condition_column = extra_condition_column
        # Lets a renamed payload key be accepted alongside the configured one, so
        # subscriptions stored under either key keep working.
        self.fallback_data_key = fallback_data_key

    def _get_value(self, metadata: Mapping[str, Any]) -> Any:
        for key in (self.extra_condition_data_key, self.fallback_data_key):
            if key is not None and key in metadata:
                return metadata[key]
        raise InvalidQueryException(
            f"'{self.extra_condition_data_key}' not found in metadata: {metadata}"
        )

    def to_dict(self, metadata: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.extra_condition_data_key: self._get_value(metadata)}

    def process(
        self,
        query: CompositeQuery[Entity] | Query,
        metadata: Mapping[str, Any],
        offset: int | None = None,
    ) -> None:
        condition_to_add: Expression = binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, self.extra_condition_column),
            Literal(None, self._get_value(metadata)),
        )
        condition = query.get_condition()
        if condition:
            new_condition = binary_condition(BooleanFunctions.AND, condition, condition_to_add)
            query.set_ast_condition(new_condition)
        else:
            query.set_ast_condition(condition_to_add)
