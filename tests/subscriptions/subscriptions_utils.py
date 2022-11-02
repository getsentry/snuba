from datetime import timedelta
from uuid import UUID

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entity_subscriptions.entity_subscription import (
    EntitySubscription,
    EventsSubscription,
)
from snuba.datasets.entity_subscriptions.factory import get_entity_subscription
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)

UUIDS = [
    UUID("fac82541-049f-4435-982d-819082761a53"),
    UUID("49215ec6-939e-41e9-a209-f09b5514e884"),
]


def build_subscription(resolution: timedelta, sequence: int) -> Subscription:
    entity_subscription = EventsSubscription()
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), UUIDS[sequence]),
        SubscriptionData(
            project_id=1,
            org_id=None,
            time_window_sec=int(timedelta(minutes=5).total_seconds()),
            resolution_sec=int(resolution.total_seconds()),
            query="MATCH events SELECT count()",
            entity_subscription=entity_subscription,
        ),
    )


def create_entity_subscription(
    entity_key: EntityKey = EntityKey.EVENTS,
) -> EntitySubscription:
    return get_entity_subscription(entity_key)()
