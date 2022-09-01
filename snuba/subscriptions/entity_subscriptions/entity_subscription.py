from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional, Sequence, Union

from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.validation.validators import (
    NoTimeBasedConditionValidator,
    SubscriptionAllowedClausesValidator,
)


class InvalidSubscriptionError(Exception):
    pass


class EntitySubscription(ABC):
    def __init__(self, data_dict: Mapping[str, Any]) -> None:
        ...

    @abstractmethod
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None
    ) -> Sequence[Expression]:
        """
        Returns a list of extra conditions that are entity specific and required for the
        snql subscriptions
        """
        raise NotImplementedError

    @abstractmethod
    def validate_query(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        """
        Applies entity specific validations on query argument passed
        """
        raise NotImplementedError

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        raise NotImplementedError


class EntitySubscriptionValidation:
    MAX_ALLOWED_AGGREGATIONS: int = 1
    disallowed_aggregations: Sequence[str] = ["groupby", "having", "orderby"]

    def validate_query(self, query: Union[CompositeQuery[Entity], Query]) -> None:
        # TODO: Support composite queries with multiple entities.
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, Entity):
            raise InvalidSubscriptionError("Only simple queries are supported")
        entity = get_entity(from_clause.key)

        SubscriptionAllowedClausesValidator(
            self.MAX_ALLOWED_AGGREGATIONS, self.disallowed_aggregations
        ).validate(query)
        if entity.required_time_column:
            NoTimeBasedConditionValidator(entity.required_time_column).validate(query)


class SessionsSubscription(EntitySubscriptionValidation, EntitySubscription):
    MAX_ALLOWED_AGGREGATIONS: int = 2

    def __init__(self, data_dict: Mapping[str, Any]) -> None:
        super().__init__(data_dict)
        try:
            self.organization: int = data_dict["organization"]
        except KeyError:
            raise InvalidQueryException(
                "organization param is required for any query over sessions entity"
            )

    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None
    ) -> Sequence[Expression]:
        return [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, self.organization),
            ),
        ]

    def to_dict(self) -> Mapping[str, Any]:
        return {"organization": self.organization}


class BaseEventsSubscription(EntitySubscriptionValidation, EntitySubscription, ABC):
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None
    ) -> Sequence[Expression]:
        if offset is None:
            return []

        return [
            binary_condition(
                ConditionFunctions.LTE,
                FunctionCall(
                    None,
                    "ifNull",
                    (Column(None, None, "offset"), Literal(None, 0)),
                ),
                Literal(None, offset),
            )
        ]

    def to_dict(self) -> Mapping[str, Any]:
        return {}


class EventsSubscription(BaseEventsSubscription):
    ...


class TransactionsSubscription(BaseEventsSubscription):
    ...


class MetricsCountersSubscription(SessionsSubscription):
    MAX_ALLOWED_AGGREGATIONS: int = 3
    disallowed_aggregations = ["having", "orderby"]


class MetricsSetsSubscription(SessionsSubscription):
    MAX_ALLOWED_AGGREGATIONS: int = 3
    disallowed_aggregations = ["having", "orderby"]


class GenericMetricsSetsSubscription(SessionsSubscription):
    MAX_ALLOWED_AGGREGATIONS: int = 3
    disallowed_aggregations = ["having", "orderby"]


class GenericMetricsDistributionsSubscription(SessionsSubscription):
    MAX_ALLOWED_AGGREGATIONS: int = 3
    disallowed_aggregations = ["having", "orderby"]
