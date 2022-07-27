import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import MutableSequence, Sequence

from snuba import settings

CLICKHOUSE_PARTITION_RE = re.compile("\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])")


class OptimizedSchedulerTimeout(Exception):
    """
    The exception raised when the optimize scheduler is called to get the
    next schedule after the final optimize job cutoff time.
    """

    pass


@dataclass(frozen=True)
class OptimizationSchedule:
    partitions: Sequence[Sequence[str]]
    cutoff_time: datetime


class OptimizeScheduler:
    """
    The optimized scheduler provides a mechanism for scheduling optimizations
    based on the parallelism desired.

    If there is no parallelism desired, then it returns a single schedule
    with the partitions provided.

    If there is parallelism desired, then there is logic which determines
    when parallelism can kick in and when it has to end. This is required
    to avoid having too much load on the database.

    If the scheduler is called to get next schedule after the last optimization
    cutoff time then OptimizedSchedulerTimeout exception is raised.
    """

    def __init__(self, parallel: int) -> None:
        self.__parallel = parallel
        self.__last_midnight = (datetime.now() + timedelta(minutes=10)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        self.__parallel_start_time = (
            self.__last_midnight + settings.PARALLEL_OPTIMIZE_JOB_START_TIME
        )
        self.__parallel_end_time = (
            self.__last_midnight + settings.PARALLEL_OPTIMIZE_JOB_END_TIME
        )
        self.__full_job_end_time = (
            self.__last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME
        )

    @staticmethod
    def sort_partitions(partitions: Sequence[str]) -> Sequence[str]:
        def sort_ordering_key(partition_name: str) -> str:
            match = re.search(CLICKHOUSE_PARTITION_RE, partition_name)
            if match is not None:
                return match.group()

            return partition_name

        return sorted(partitions, key=sort_ordering_key, reverse=True)

    @staticmethod
    def subdivide_partitions(
        partitions: Sequence[str], number_of_subdivisions: int
    ) -> Sequence[Sequence[str]]:
        """
        Subdivide a list of partitions into number_of_subdivisions lists
        of partitions so that optimizations can be executed in parallel.

        We sort the partitions so that the more recent partitions are
        optimized first since more recent partitions might have more
        replacements.
        """

        sorted_partitions = OptimizeScheduler.sort_partitions(partitions)
        output: MutableSequence[Sequence[str]] = []

        for i in range(number_of_subdivisions):
            output.append(sorted_partitions[i::number_of_subdivisions])

        return output

    def get_next_schedule(self, partitions: Sequence[str]) -> OptimizationSchedule:
        """
        Get the next schedule for optimizing partitions. The provided partitions
        are subdivided into parallel number of partitions and the cutoff time
        for each schedule is determined by when parallelism boundaries are
        reached.
        """
        current_time = datetime.now()
        if current_time >= self.__full_job_end_time:
            raise OptimizedSchedulerTimeout(
                f"Optimize job cutoff time exceeded "
                f"{self.__full_job_end_time}. Abandoning"
            )

        if self.__parallel == 1:
            return OptimizationSchedule(
                partitions=[self.sort_partitions(partitions)],
                cutoff_time=self.__last_midnight + settings.OPTIMIZE_JOB_CUTOFF_TIME,
            )
        else:
            if current_time < self.__parallel_start_time:
                return OptimizationSchedule(
                    partitions=[self.sort_partitions(partitions)],
                    cutoff_time=self.__parallel_start_time,
                )
            elif current_time < self.__parallel_end_time:
                return OptimizationSchedule(
                    partitions=self.subdivide_partitions(partitions, self.__parallel),
                    cutoff_time=self.__parallel_end_time,
                )
            else:
                return OptimizationSchedule(
                    partitions=[self.sort_partitions(partitions)],
                    cutoff_time=self.__full_job_end_time,
                )
