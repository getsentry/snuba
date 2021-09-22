from datetime import timedelta
from uuid import UUID

from snuba.subscriptions.data import (
    PartitionId,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.entity_subscription import (
    EventsSubscription,
    SessionsSubscription,
    SubscriptionType,
)

UUIDS = [
    UUID("fac82541-049f-4435-982d-819082761a53"),
    UUID("49215ec6-939e-41e9-a209-f09b5514e884"),
]


def build_subscription(resolution: timedelta, sequence: int) -> Subscription:
    entity_subscription = EventsSubscription(
        subscription_type=SubscriptionType.SNQL, data_dict={}
    )
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), UUIDS[sequence]),
        SnQLSubscriptionData(
            project_id=1,
            time_window=timedelta(minutes=5),
            resolution=resolution,
            query="MATCH events SELECT count()",
            entity_subscription=entity_subscription,
        ),
    )


def create_entity_subscription(subscription_type, dataset_name="events"):
    if dataset_name == "sessions":
        return SessionsSubscription(
            subscription_type=subscription_type, data_dict={"organization": 1}
        )
    else:
        return EventsSubscription(subscription_type=subscription_type, data_dict={})
