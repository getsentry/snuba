from abc import ABC, abstractmethod
from typing import Optional, Sequence, Union

from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.validation.validators import (
    NoTimeBasedConditionValidator,
    SubscriptionAllowedClausesValidator,
)


class InvalidSubscriptionError(Exception):
    pass


class EntitySubscription(ABC):
    def __init__(self) -> None:
        ...

    @abstractmethod
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None, organization: Optional[int] = None
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

    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None, organization: Optional[int] = None
    ) -> Sequence[Expression]:
        if not organization:
            raise InvalidQueryException("organization param is required")
        return [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "org_id"),
                Literal(None, organization),
            ),
        ]


class EventsSubscription(EntitySubscriptionValidation, EntitySubscription):
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None, organization: Optional[int] = None
    ) -> Sequence[Expression]:
        return []


class TransactionsSubscription(EntitySubscriptionValidation, EntitySubscription):
    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None, organization: Optional[int] = None
    ) -> Sequence[Expression]:
        return []


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
