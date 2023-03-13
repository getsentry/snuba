from datetime import datetime, timedelta
from typing import Sequence

import pytest
from freezegun import freeze_time

from snuba import settings
from snuba.clickhouse.optimize.optimize_scheduler import (
    OptimizationSchedule,
    OptimizedSchedulerTimeout,
    OptimizeScheduler,
)


@pytest.mark.parametrize(
    "partitions,subdivisions,expected",
    [
        pytest.param(
            ["(90,'2022-03-28')"],
            2,
            [["(90,'2022-03-28')"], []],
            id="one part",
        ),
        pytest.param(
            [
                "(90,'2022-03-28')",
                "(90,'2022-03-21')",
            ],
            2,
            [
                ["(90,'2022-03-28')"],
                ["(90,'2022-03-21')"],
            ],
            id="two partitions",
        ),
        pytest.param(
            [
                "(90,'2022-03-28')",
                "(90,'2022-03-21')",
                "(30,'2022-03-28')",
            ],
            2,
            [
                [
                    "(90,'2022-03-28')",
                    "(90,'2022-03-21')",
                ],
                [
                    "(30,'2022-03-28')",
                ],
            ],
            id="three partitions",
        ),
        pytest.param(
            [
                "(90,'2022-03-28')",
                "(90,'2022-03-21')",
                "(30,'2022-03-28')",
                "(30,'2022-03-21')",
            ],
            2,
            [
                [
                    "(90,'2022-03-28')",
                    "(90,'2022-03-21')",
                ],
                [
                    "(30,'2022-03-28')",
                    "(30,'2022-03-21')",
                ],
            ],
            id="four partitions",
        ),
        pytest.param(
            [
                "(90,'2022-03-28')",
                "(90,'2022-03-21')",
                "(30,'2022-03-28')",
                "(30,'2022-03-21')",
                "(90,'2022-03-14')",
                "(90,'2022-03-07')",
            ],
            3,
            [
                [
                    "(90,'2022-03-28')",
                    "(30,'2022-03-21')",
                ],
                [
                    "(30,'2022-03-28')",
                    "(90,'2022-03-14')",
                ],
                [
                    "(90,'2022-03-21')",
                    "(90,'2022-03-07')",
                ],
            ],
            id="six partitions",
        ),
        pytest.param(
            [
                "(90,'2022-03-07')",
                "(90,'2022-03-28')",
                "(90,'2022-03-21')",
                "(30,'2022-03-28')",
                "(90,'2022-03-14')",
                "(30,'2022-03-21')",
            ],
            1,
            [
                [
                    "(90,'2022-03-28')",
                    "(30,'2022-03-28')",
                    "(90,'2022-03-21')",
                    "(30,'2022-03-21')",
                    "(90,'2022-03-14')",
                    "(90,'2022-03-07')",
                ]
            ],
            id="six partitions non-sorted",
        ),
    ],
)
def test_subdivide_partitions(
    partitions: Sequence[str],
    subdivisions: int,
    expected: Sequence[Sequence[str]],
) -> None:
    optimize_scheduler = OptimizeScheduler(parallel=1)
    assert optimize_scheduler.subdivide_partitions(partitions, subdivisions) == expected


@pytest.mark.parametrize(
    "parallel,expected",
    [
        pytest.param(
            1,
            [0],
            id="no parallel",
        ),
        pytest.param(
            2,
            [0, settings.OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES],
            id="2 parallel",
        ),
        pytest.param(
            3,
            [
                0,
                settings.OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES / 2,
                settings.OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES,
            ],
            id="3 parallel",
        ),
    ],
)
def test_start_time_jitter(parallel: int, expected: Sequence[int]) -> None:
    scheduler = OptimizeScheduler(parallel=parallel)
    assert scheduler.start_time_jitter() == expected


last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
    hour=0, minute=0, second=0, microsecond=0
)


@pytest.mark.parametrize(
    "parallel,partitions,current_time,expected",
    [
        pytest.param(
            1,
            [
                "(90,'2022-06-13')",
                "(90,'2022-06-08')",
                "(90,'2022-06-20')",
                "(90,'2022-06-27')",
            ],
            last_midnight + timedelta(minutes=30),
            OptimizationSchedule(
                [
                    [
                        "(90,'2022-06-27')",
                        "(90,'2022-06-20')",
                        "(90,'2022-06-13')",
                        "(90,'2022-06-08')",
                    ]
                ],
                last_midnight + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME),
            ),
            id="non parallel",
        ),
        pytest.param(
            2,
            [
                "(90,'2022-06-13')",
                "(30,'2022-06-08')",
                "(30,'2022-06-20')",
                "(90,'2022-06-27')",
            ],
            last_midnight
            + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_START_TIME)
            - timedelta(minutes=30),
            OptimizationSchedule(
                [
                    [
                        "(90,'2022-06-27')",
                        "(30,'2022-06-20')",
                        "(90,'2022-06-13')",
                        "(30,'2022-06-08')",
                    ]
                ],
                last_midnight
                + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_START_TIME),
            ),
            id="parallel before parallel start",
        ),
        pytest.param(
            2,
            ["(90,'2022-03-28')", "(90,'2022-03-21')"],
            last_midnight
            + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_END_TIME)
            - timedelta(minutes=30),
            OptimizationSchedule(
                [["(90,'2022-03-28')"], ["(90,'2022-03-21')"]],
                last_midnight
                + timedelta(hours=settings.PARALLEL_OPTIMIZE_JOB_END_TIME),
                [0, settings.OPTIMIZE_PARALLEL_MAX_JITTER_MINUTES],
            ),
            id="parallel before parallel end",
        ),
        pytest.param(
            2,
            ["(90,'2022-03-28')", "(90,'2022-03-21')"],
            last_midnight
            + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME)
            - timedelta(minutes=30),
            OptimizationSchedule(
                [["(90,'2022-03-28')", "(90,'2022-03-21')"]],
                last_midnight + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME),
            ),
            id="parallel before final cutoff",
        ),
    ],
)
def test_get_next_schedule(
    parallel: int,
    partitions: Sequence[str],
    current_time: datetime,
    expected: OptimizationSchedule,
) -> None:
    optimize_scheduler = OptimizeScheduler(parallel=parallel)

    with freeze_time(current_time):
        assert optimize_scheduler.get_next_schedule(partitions) == expected


def test_get_next_schedule_raises_exception() -> None:
    optimize_scheduler = OptimizeScheduler(parallel=1)
    with freeze_time(
        last_midnight
        + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME)
        + timedelta(minutes=20)
    ):
        with pytest.raises(OptimizedSchedulerTimeout):
            optimize_scheduler.get_next_schedule(
                ["(90,'2022-03-28')", "(90,'2022-03-21')"]
            )
