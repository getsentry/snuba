from datetime import timedelta
from typing import Optional
from uuid import UUID

from snuba.datasets.entities import EntityKey, EntityKeys
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
    SubscriptionIdentifier,
)
from snuba.subscriptions.entity_subscription import (
    ENTITY_KEY_TO_SUBSCRIPTION_MAPPER,
    EntitySubscription,
    EventsSubscription,
)

UUIDS = [
    UUID("fac82541-049f-4435-982d-819082761a53"),
    UUID("49215ec6-939e-41e9-a209-f09b5514e884"),
]


def build_subscription(resolution: timedelta, sequence: int) -> Subscription:
    entity_subscription = EventsSubscription(data_dict={})
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), UUIDS[sequence]),
        SubscriptionData(
            project_id=1,
            time_window_sec=int(timedelta(minutes=5).total_seconds()),
            resolution_sec=int(resolution.total_seconds()),
            query="MATCH events SELECT count()",
            entity_subscription=entity_subscription,
        ),
    )


def create_entity_subscription(
    entity_key: EntityKey = EntityKeys.EVENTS, org_id: Optional[int] = None
) -> EntitySubscription:
    if org_id:
        data_dict = {"organization": org_id}
    else:
        data_dict = {}
    return ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[entity_key](data_dict=data_dict)
