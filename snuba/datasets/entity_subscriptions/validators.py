from abc import abstractmethod
from typing import Optional, Sequence, Type, Union, cast

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
    def validate(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        raise NotImplementedError


class AggregationValidator(EntitySubscriptionValidator):
    def __init__(
        self,
        max_allowed_aggregations: int,
        disallowed_aggregations: Sequence[str],
        required_time_column: Optional[str] = None,
        allows_group_by_without_condition: bool = False,
    ):
        self.max_allowed_aggregations = max_allowed_aggregations
        self.disallowed_aggregations = disallowed_aggregations
        self.required_time_column = required_time_column
        self.allows_group_by_without_condition = allows_group_by_without_condition

    def validate(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        SubscriptionAllowedClausesValidator(
            self.max_allowed_aggregations,
            self.disallowed_aggregations,
            allows_group_by_without_condition=self.allows_group_by_without_condition,
        ).validate(query)
        if self.required_time_column:
            NoTimeBasedConditionValidator(self.required_time_column).validate(query)
