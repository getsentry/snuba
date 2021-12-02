from datetime import datetime, timedelta
from typing import Sequence, Tuple

import pytest

from snuba import state
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import (
    DelegateTaskBuilder,
    ImmediateTaskBuilder,
    JitteredTaskBuilder,
    Tags,
    TaskBuilder,
)
from snuba.subscriptions.utils import Tick
from snuba.utils.scheduler import ScheduledTask
from snuba.utils.types import Interval
from tests.subscriptions.subscriptions_utils import UUIDS, build_subscription

ALIGNED_TIMESTAMP = 1625518080  # Aligned to start of a minute


def build_tick(lower: int, upper: int) -> Tick:
    return Tick(
        None,
        Interval(0, 1),
        Interval(datetime.fromtimestamp(lower), datetime.fromtimestamp(upper)),
    )


TEST_CASES = [
    pytest.param(
        ImmediateTaskBuilder(),
        "jittered",
        [(ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 0))],
        [
            (
                ALIGNED_TIMESTAMP,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    (
                        build_subscription(timedelta(minutes=1), 0),
                        build_tick(ALIGNED_TIMESTAMP, ALIGNED_TIMESTAMP + 60),
                    ),
                ),
            )
        ],
        [("tasks.built", 1, {})],
        id="One subscription immediately scheduled",
    ),
    pytest.param(
        ImmediateTaskBuilder(),
        "jittered",
        [(ALIGNED_TIMESTAMP + 1, build_subscription(timedelta(minutes=1), 0))],
        [],
        [("tasks.built", 0, {})],
        id="One subscription not aligned with resolution",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        "jittered",
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
                    (
                        build_subscription(timedelta(minutes=1), 0),
                        build_tick(
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                        ),
                    ),
                ),
            )
        ],
        [("tasks.built", 1, {}), ("tasks.above.resolution", 0, {})],
        id="One subscription scheduled with jitter. Happens at 43rd second",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        "jittered",
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
                    (
                        build_subscription(timedelta(minutes=1), 0),
                        build_tick(
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                        ),
                    ),
                ),
            )
        ],
        [("tasks.built", 1, {}), ("tasks.above.resolution", 0, {})],
        id="Ensures different ids generate different jitters",
    ),
    pytest.param(
        JitteredTaskBuilder(),
        "jittered",
        [(ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=2), 0))],
        [
            (
                ALIGNED_TIMESTAMP,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    (
                        build_subscription(timedelta(minutes=2), 0),
                        build_tick(ALIGNED_TIMESTAMP, ALIGNED_TIMESTAMP + 60),
                    ),
                ),
            )
        ],
        [("tasks.built", 1, {}), ("tasks.above.resolution", 1, {})],
        id="Do not apply jitter if resolution is above threshold",
    ),
    pytest.param(
        DelegateTaskBuilder(),
        "jittered",
        [
            (ALIGNED_TIMESTAMP, build_subscription(timedelta(minutes=1), 0)),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                build_subscription(timedelta(minutes=1), 0),
            ),
        ],
        [
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP),
                    (
                        build_subscription(timedelta(minutes=1), 0),
                        build_tick(
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                        ),
                    ),
                ),
            )
        ],
        [
            ("tasks.built", 1, {"type": "immediate"}),
            ("tasks.built", 1, {"type": "jittered"}),
            ("tasks.above.resolution", 0, {"type": "jittered"}),
        ],
        id="Delegate returns the jittered one.",
    ),
    pytest.param(
        DelegateTaskBuilder(),
        "transition_jitter",
        [
            (ALIGNED_TIMESTAMP + 30, build_subscription(timedelta(minutes=1), 0)),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60,
                build_subscription(timedelta(minutes=1), 0),
            ),
            (ALIGNED_TIMESTAMP + 60, build_subscription(timedelta(minutes=1), 0)),
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                build_subscription(timedelta(minutes=1), 0),
            ),
            (ALIGNED_TIMESTAMP + 120, build_subscription(timedelta(minutes=1), 0)),
        ],
        [
            (
                ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                ScheduledTask(
                    datetime.fromtimestamp(ALIGNED_TIMESTAMP + 60),
                    (
                        build_subscription(timedelta(minutes=1), 0),
                        build_tick(
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 60,
                            ALIGNED_TIMESTAMP + UUIDS[0].int % 60 + 120,
                        ),
                    ),
                ),
            )
        ],
        [
            ("tasks.built", 2, {"type": "immediate"}),
            ("tasks.built", 2, {"type": "jittered"}),
            ("tasks.above.resolution", 0, {"type": "jittered"}),
        ],
        id="Delegate transitions to jittered mode.",
    ),
]


@pytest.mark.parametrize(
    "builder, primary_builder_config, sequence_in, task_sequence, metrics", TEST_CASES
)
def test_sequences(
    builder: TaskBuilder[Subscription],
    primary_builder_config: str,
    sequence_in: Sequence[Tuple[int, Subscription]],
    task_sequence: Sequence[Tuple[int, ScheduledTask[Tuple[Subscription, Tick]]]],
    metrics: Sequence[Tuple[str, int, Tags]],
) -> None:
    """
    Tries to execute the task builder on several sequences of
    subscriptions and validate the proper jitter is applied.
    state.
    """
    state.set_config("subscription_primary_task_builder", primary_builder_config)
    output = []
    for timestamp, subscription in sequence_in:
        tick = Tick(
            None,
            Interval(0, 1),
            Interval(
                datetime.fromtimestamp(timestamp),
                datetime.fromtimestamp(timestamp) + timedelta(minutes=1),
            ),
        )
        ret = builder.get_task((subscription, tick), timestamp)
        if ret:
            output.append((timestamp, ret))

    assert output == task_sequence
    assert builder.reset_metrics() == metrics
