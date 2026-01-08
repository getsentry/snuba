import importlib
import uuid
from datetime import timedelta
from random import randint
from typing import MutableSequence
from unittest.mock import patch

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.subscriptions import scheduler
from snuba.subscriptions.data import (
    PartitionId,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import filter_subscriptions
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend


def build_subscription(resolution: timedelta, org_id: int) -> Subscription:
    return Subscription(
        SubscriptionIdentifier(PartitionId(1), uuid.uuid4()),
        SnQLSubscriptionData(
            project_id=1,
            query="MATCH (events) SELECT count() AS count",
            time_window_sec=60,
            resolution_sec=int(resolution.total_seconds()),
            entity=get_entity(EntityKey.EVENTS),
            metadata={"organization": org_id},
        ),
    )


@pytest.fixture
def expected_subs() -> MutableSequence[Subscription]:
    return [build_subscription(timedelta(minutes=1), 2) for count in range(randint(1, 50))]


@pytest.fixture
def extra_subs() -> MutableSequence[Subscription]:
    return [build_subscription(timedelta(minutes=3), 1) for count in range(randint(1, 50))]


@patch("snuba.settings.SLICED_STORAGE_SETS", {"events": 3})
@patch("snuba.settings.LOGICAL_PARTITION_MAPPING", {"events": {0: 0, 1: 1, 2: 2}})
def test_filter_subscriptions(expected_subs, extra_subs) -> None:  # type: ignore
    importlib.reload(scheduler)

    filtered_subs = filter_subscriptions(
        subscriptions=expected_subs + extra_subs,
        entity_key=EntityKey.EVENTS,
        metrics=DummyMetricsBackend(strict=True),
        slice_id=2,
    )
    assert filtered_subs == expected_subs
