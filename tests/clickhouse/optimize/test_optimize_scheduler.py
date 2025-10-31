from datetime import UTC, datetime, timedelta
from typing import Sequence

import pytest
import time_machine

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
    optimize_scheduler = OptimizeScheduler(default_parallel_threads=1)
    assert (
        optimize_scheduler._subdivide_partitions(partitions, subdivisions) == expected
    )


last_midnight = (datetime.now(UTC) + timedelta(minutes=10)).replace(
    hour=0, minute=0, second=0, microsecond=0
)


@pytest.mark.parametrize(
    "default_parallel_threads,partitions,current_time,expected",
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
@pytest.mark.xfail(
    reason="This test still is flaky sometimes and then completely blocks CI / deployment"
)
def test_get_next_schedule(
    default_parallel_threads: int,
    partitions: Sequence[str],
    current_time: datetime,
    expected: OptimizationSchedule,
) -> None:
    optimize_scheduler = OptimizeScheduler(
        default_parallel_threads=default_parallel_threads
    )

    with time_machine.travel(current_time, tick=False):
        assert optimize_scheduler.get_next_schedule(partitions) == expected


def test_get_next_schedule_raises_exception() -> None:
    with time_machine.travel(last_midnight, tick=False):
        optimize_scheduler = OptimizeScheduler(default_parallel_threads=1)
        with time_machine.travel(
            last_midnight
            + timedelta(hours=settings.OPTIMIZE_JOB_CUTOFF_TIME)
            + timedelta(minutes=20),
            tick=False,
        ):
            with pytest.raises(OptimizedSchedulerTimeout):
                optimize_scheduler.get_next_schedule(
                    ["(90,'2022-03-28')", "(90,'2022-03-21')"]
                )
