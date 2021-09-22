from abc import ABC
from enum import Enum
from typing import Any, List, Mapping, Optional, Sequence, Type, Union

from snuba.datasets.entities import EntityKey
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.types import Condition


class SubscriptionType(Enum):
    LEGACY = "legacy"
    SNQL = "snql"
    DELEGATE = "delegate"


class EntitySubscription(ABC):
    def __init__(
        self, subscription_type: SubscriptionType, data_dict: Mapping[str, Any]
    ) -> None:
        self.subscription_type = subscription_type

    def get_entity_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> Sequence[Union[Expression, Condition]]:
        return []

    def to_dict(self) -> Mapping[str, Any]:
        return {}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EntitySubscription):
            return NotImplemented
        return self.to_dict() == other.to_dict() and isinstance(other, type(self))


class SessionsSubscription(EntitySubscription):
    organization: int

    def __init__(
        self, subscription_type: SubscriptionType, data_dict: Mapping[str, Any]
    ):
        super().__init__(subscription_type, data_dict)
        try:
            self.organization = data_dict["organization"]
        except KeyError:
            raise InvalidQueryException(
                "organization param is required for any query over sessions entity"
            )

    def get_entity_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> Sequence[Union[Expression, Condition]]:
        """
        Returns a list of extra conditions that are entity specific and required for the
        subscription
        """
        conditions_to_add = []
        if self.subscription_type == SubscriptionType.SNQL:
            conditions_to_add += [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "org_id"),
                    Literal(None, self.organization),
                ),
            ]
        return conditions_to_add

    def to_dict(self) -> Mapping[str, Any]:
        return {"organization": self.organization}


class EventsSubscription(EntitySubscription):
    def get_entity_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> Sequence[Union[Expression, Condition]]:
        """
        Returns a list of extra conditions that are entity specific and required for the
        subscription
        """
        if offset is None:
            return []

        conditions_to_add: List[Union[Expression, Condition]] = []
        if self.subscription_type == SubscriptionType.SNQL:
            conditions_to_add.append(
                binary_condition(
                    ConditionFunctions.LTE,
                    FunctionCall(
                        None,
                        "ifNull",
                        (Column(None, None, "offset"), Literal(None, 0)),
                    ),
                    Literal(None, offset),
                )
            )
        elif self.subscription_type == SubscriptionType.LEGACY:
            conditions_to_add = [[["ifnull", ["offset", 0]], "<=", offset]]
        return conditions_to_add


class TransactionsSubscription(EventsSubscription):
    ...


def get_entity_subscription_class_from_dataset_name(
    dataset_name: str,
) -> Type[EntitySubscription]:
    """
    Function that returns the correct EntitySubscription class based on the provided dictionary
    of data
    """
    return ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[EntityKey(dataset_name)]


def get_dataset_name_from_entity_subscription_class(
    entity_subscription_type: Type[EntitySubscription],
) -> str:
    return ENTITY_SUBSCRIPTION_TO_KEY_MAPPER[entity_subscription_type].value


ENTITY_SUBSCRIPTION_TO_KEY_MAPPER: Mapping[Type[EntitySubscription], EntityKey] = {
    SessionsSubscription: EntityKey.SESSIONS,
    EventsSubscription: EntityKey.EVENTS,
    TransactionsSubscription: EntityKey.TRANSACTIONS,
}

ENTITY_KEY_TO_SUBSCRIPTION_MAPPER: Mapping[EntityKey, Type[EntitySubscription]] = {
    EntityKey.SESSIONS: SessionsSubscription,
    EntityKey.EVENTS: EventsSubscription,
    EntityKey.TRANSACTIONS: TransactionsSubscription,
}
