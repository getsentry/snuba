from abc import abstractmethod
from typing import Any, Mapping, Sequence, Type, Union, cast

from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.logical import Query
from snuba.query.validation.validators import (
    NoTimeBasedConditionValidator,
    SubscriptionAllowedClausesValidator,
)
from snuba.utils.registered_class import RegisteredClass


class InvalidSubscriptionError(Exception):
    pass


class EntitySubscriptionValidator(metaclass=RegisteredClass):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["EntitySubscriptionValidator"]:
        return cast(Type["EntitySubscriptionValidator"], cls.class_from_name(name))

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def validate(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        raise NotImplementedError


class AggregationValidator(EntitySubscriptionValidator):
    def __init__(
        self, max_allowed_aggregations: int, disallowed_aggregations: Sequence[str]
    ):
        self.max_allowed_aggregations = max_allowed_aggregations
        self.disallowed_aggregations = disallowed_aggregations

    def to_dict(self) -> Mapping[str, Any]:
        return {}

    def validate(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        from snuba.datasets.entities.factory import get_entity

        from_clause = query.get_from_clause()
        if not isinstance(from_clause, Entity):
            raise InvalidSubscriptionError("Only simple queries are supported")
        entity = get_entity(from_clause.key)

        SubscriptionAllowedClausesValidator(
            self.max_allowed_aggregations, self.disallowed_aggregations
        ).validate(query)
        if entity.required_time_column:
            NoTimeBasedConditionValidator(entity.required_time_column).validate(query)
