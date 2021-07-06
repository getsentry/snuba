from datetime import timedelta
from uuid import UUID

from snuba.subscriptions.data import (
    PartitionId,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
)

UUIDS = [
    UUID("fac82541-049f-4435-982d-819082761a53"),
    UUID("49215ec6-939e-41e9-a209-f09b5514e884"),
]


def build_subscription(resolution: timedelta, sequence: int) -> Subscription:
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), UUIDS[sequence]),
        SnQLSubscriptionData(
            project_id=1,
            time_window=timedelta(minutes=5),
            resolution=resolution,
            query="MATCH events SELECT count()",
        ),
    )
