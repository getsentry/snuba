from dataclasses import dataclass
from typing import Optional, Sequence

from snuba.datasets.entity_subscriptions.processors import EntitySubscriptionProcessor
from snuba.datasets.entity_subscriptions.validators import EntitySubscriptionValidator


class InvalidSubscriptionError(Exception):
    pass


@dataclass
class EntitySubscription:
    processors: Optional[Sequence[EntitySubscriptionProcessor]]
    validators: Optional[Sequence[EntitySubscriptionValidator]]
