from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Union

from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as EntityDS
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.query.validation.validators import (
    NoTimeBasedConditionValidator,
    SubscriptionAllowedClausesValidator,
)


class InvalidSubscriptionError(Exception):
    pass


class EntitySubscription:
    """
    Used to provide extra functionality for entity specific subscriptions.
    An instance of this class is set as an attribute of Entity.
        Entity.entity_subscription: Optional[EntitySubscription]
    """

    def __init__(
        self,
        max_allowed_aggregations: int = 1,
        disallowed_aggregations: Sequence[str] = ["groupby", "having", "orderby"],
    ) -> None:
        self.max_allowed_aggregations = max_allowed_aggregations
        self.disallowed_aggregations = disallowed_aggregations

    def validate_query(self, query: Union[CompositeQuery[EntityDS], Query]) -> None:
        # Import get_entity() when used, not at import time to avoid circular imports
        from snuba.datasets.entities.factory import get_entity

        # TODO: Support composite queries with multiple entities.
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, EntityDS):
            raise InvalidSubscriptionError("Only simple queries are supported")
        entity = get_entity(from_clause.key)

        SubscriptionAllowedClausesValidator(
            self.max_allowed_aggregations, self.disallowed_aggregations
        ).validate(query)
        if entity.required_time_column:
            NoTimeBasedConditionValidator(entity.required_time_column).validate(query)

    def get_entity_subscription_conditions_for_snql(
        self, offset: Optional[int] = None, organization: Optional[int] = None
    ) -> Sequence[Expression]:
        if organization:
            return [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "org_id"),
                    Literal(None, organization),
                ),
            ]
        return []

    def to_dict(self) -> Mapping[str, Any]:
        return self.__dict__
