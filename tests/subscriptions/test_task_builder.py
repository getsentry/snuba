from datetime import datetime, timedelta
from typing import Sequence, Tuple
from uuid import UUID

import pytest

from snuba.subscriptions.data import (
    PartitionId,
    SnQLSubscriptionData,
    Subscription,
    SubscriptionIdentifier,
)
from snuba.subscriptions.scheduler import (
    ImmediateTaskBuilder,
    JitteredTaskBuilder,
    TaskBuilder,
)
from snuba.utils.scheduler import ScheduledTask

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


ALIGNED_TIMESTAMP = 1625518080  # Aligned to start of a minute

TEST_CASES = [
    pytest.param(
        ImmediateTaskBuilder(),
        [(ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 0))],
        [
            (
                ALIGNED_TIMESTAMP,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    build_subscription(timedelta(minutes=1), 0),
                ),
            )
        ],
        id="One subscription immediately scheduled",
    ),
    pytest.param(
        ImmediateTaskBuilder(),
        [(ALIGNED_TIMESTAMP + 1, build_subscription(timedelta(minutes=1), 0))],
        [],
        id="One subscription not aligned with resolution",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        [
            (ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 0)),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                build_subscription(timedelta(minutes=1), 0),
            ),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 1,
                build_subscription(timedelta(minutes=1), 0),
            ),
        ],
        [
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                ScheduledTask(
                    # Notice the timestamp of the task is the one without
                    # jitter so that the query time range is still aligned
                    # to the minute without jitter.
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    build_subscription(timedelta(minutes=1), 0),
                ),
            )
        ],
        id="One subscription scheduled with jitter. Happens at 43rd second",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        [
            (ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 0)),
            (ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 1)),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                build_subscription(timedelta(minutes=1), 0),
            ),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                build_subscription(timedelta(minutes=1), 1),
            ),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 1,
                build_subscription(timedelta(minutes=1), 0),
            ),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 1,
                build_subscription(timedelta(minutes=1), 1),
            ),
        ],
        [
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    build_subscription(timedelta(minutes=1), 0),
                ),
            )
        ],
        id="Ensures different ids generate different jitters",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        [(ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=2), 0))],
        [
            (
                ALIGNED_TIMESTAMP,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    build_subscription(timedelta(minutes=2), 0),
                ),
            )
        ],
        id="Do not apply jitter if resolution is above threshold",
    ),
]


@pytest.mark.parametrize("builder, sequence_in, task_sequence", TEST_CASES)
def test_sequences(
    builder: TaskBuilder[Subscription],
    sequence_in: Sequence[Tuple[int, Subscription]],
    task_sequence: Sequence[Tuple[int, ScheduledTask[Subscription]]],
) -> None:
    """
    Tries to execute the task builder on several sequences of
    subscriptions and validate the proper jitter is applied.
    """

    output = []
    for timestamp, subscription in sequence_in:
        ret = builder.get_task(subscription, timestamp)
        if ret:
            output.append((timestamp, ret))

    assert output == task_sequence
