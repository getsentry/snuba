from datetime import timedelta
from typing import Sequence, Tuple

import pytest

from snuba import settings, state
from snuba.subscriptions.data import Subscription
from snuba.subscriptions.scheduler import TaskBuilderMode, TaskBuilderModeState
from tests.subscriptions.subscriptions_utils import build_subscription

ALIGNED_TIMESTAMP = 1625527800  # Aligned to start of a minute

TEST_CASES = [
    pytest.param(
        "immediate",
        [(build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP)],
        [TaskBuilderMode.IMMEDIATE],
        id="Immediate general mode",
    ),
    pytest.param(
        "jittered",
        [(build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP)],
        [TaskBuilderMode.JITTERED],
        id="Jittered general mode",
    ),
    pytest.param(
        "transition_jitter",
        [
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 30),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 45),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 60),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 65),
        ],
        [
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.JITTERED,
        ],
        id="Transition of one subscription to jittered",
    ),
    pytest.param(
        "transition_immediate",
        [
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 30),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 45),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 60),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 65),
        ],
        [
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.IMMEDIATE,
        ],
        id="Transition of one subscription to immediate",
    ),
    pytest.param(
        "transition_jitter",
        [
            (build_subscription(timedelta(minutes=5), 0), ALIGNED_TIMESTAMP + 45),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 45),
            (build_subscription(timedelta(minutes=5), 0), ALIGNED_TIMESTAMP + 60),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 60),
            (build_subscription(timedelta(minutes=5), 0), ALIGNED_TIMESTAMP + 65),
            (build_subscription(timedelta(minutes=5), 0), ALIGNED_TIMESTAMP + 300),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 300),
            (build_subscription(timedelta(minutes=5), 0), ALIGNED_TIMESTAMP + 305),
            (build_subscription(timedelta(minutes=1), 0), ALIGNED_TIMESTAMP + 305),
        ],
        [
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.IMMEDIATE,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.JITTERED,
            TaskBuilderMode.JITTERED,
        ],
        id="Transitions with multiple different resolutions",
    ),
    pytest.param(
        "transition_jitter",
        [(build_subscription(timedelta(minutes=1800), 0), ALIGNED_TIMESTAMP + 1)],
        [TaskBuilderMode.JITTERED],
        id="Resolution beyond maximum. Perform transition",
    ),
]


@pytest.mark.parametrize("general_mode, subscriptions, expected_modes", TEST_CASES)
@pytest.mark.redis_db
def test_state_changes(
    general_mode: str,
    subscriptions: Sequence[Tuple[Subscription, int]],
    expected_modes: Sequence[TaskBuilderMode],
) -> None:
    prev_threshold = settings.MAX_RESOLUTION_FOR_JITTER
    settings.MAX_RESOLUTION_FOR_JITTER = 300
    state.set_config("subscription_primary_task_builder", general_mode)
    mode_state = TaskBuilderModeState()
    modes = [
        mode_state.get_current_mode(subscription, timestamp)
        for subscription, timestamp in subscriptions
    ]
    assert modes == expected_modes
    settings.MAX_RESOLUTION_FOR_JITTER = prev_threshold
