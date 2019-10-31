from dataclasses import dataclass
from typing import Union
from uuid import UUID

from snuba.subscriptions.types import Subscription


@dataclass(frozen=True)
class SubscriptionUpdateRequest:
    uuid: UUID
    subscription: Subscription


@dataclass(frozen=True)
class SubscriptionDeleteRequest:
    uuid: UUID


SubscriptionMessage = Union[SubscriptionUpdateRequest, SubscriptionDeleteRequest]
