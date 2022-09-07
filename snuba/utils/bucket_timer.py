from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Deque, List

from snuba import settings, state


def get_time_sum(group: List[timedelta]) -> timedelta:
    return sum([processing_time for processing_time in group], timedelta())


def floor_minute(time: datetime) -> datetime:
    return time - timedelta(seconds=time.second, microseconds=time.microsecond)


def ceil_minute(time: datetime) -> datetime:
    if time.second == 0 and time.microsecond == 0:
        return time
    return floor_minute(time + timedelta(minutes=1))


@dataclass
class Bucket:
    project_id: int
    minute: datetime
    processing_time: timedelta = timedelta(seconds=0)


class Counter:
    """
    The Counter class is used to track time spent on some activity (e.g. processing a replacement) for a project.
    To accomplish this, the `record_time_spent()` function captures some processing time range and splits it by a per
    minute resolution (Bucket). The buckets older than settings.COUNTER_WINDOW_SIZE are trimmed. Finally, the `get_bucket_totals_exceeding_limit()`
    function returns all project ids who's total processing time has exceeded self.limit.
    """

    def __init__(self, consumer_group: str) -> None:
        self.consumer_group: str = consumer_group
        self.buckets: Deque[Bucket] = deque()

        percentage = state.get_config("counter_window_size_percentage", 1.0)
        assert isinstance(percentage, float)
        self.limit = settings.COUNTER_WINDOW_SIZE * percentage

    def __trim_expired_buckets(self, now: datetime) -> None:
        current_minute = floor_minute(now)
        window_start = current_minute - settings.COUNTER_WINDOW_SIZE
        while self.buckets and self.buckets[0].minute < window_start:
            self.buckets.popleft()

    def __add_to_bucket(
        self,
        project_id: int,
        start_minute: datetime,
        processing_time: timedelta,
    ) -> None:
        curr_bucket = None
        for bucket in self.buckets:
            if bucket.project_id == project_id and bucket.minute == start_minute:
                curr_bucket = bucket
        if not curr_bucket:
            curr_bucket = Bucket(project_id, start_minute, processing_time)
            self.buckets.append(curr_bucket)
        else:
            curr_bucket.processing_time += processing_time

    def record_time_spent(
        self, project_id: int, start: datetime, end: datetime
    ) -> None:
        start_minute = floor_minute(start)
        left = start
        right = ceil_minute(start)
        while right <= end:
            self.__add_to_bucket(project_id, start_minute, right - left)
            left = start_minute = right
            right += timedelta(minutes=1)
        self.__add_to_bucket(project_id, start_minute, end - left)

    def get_projects_exceeding_limit(self) -> List[int]:
        now = datetime.now()
        self.__trim_expired_buckets(now)
        project_groups = defaultdict(list)
        for bucket in self.buckets:
            project_groups[bucket.project_id].append(bucket.processing_time)

        # Compare the replacement total grouped by project_id with system time limit
        projects_exceeding_time_limit = []
        for project_id, processing_times in project_groups.items():
            project_processing_time = get_time_sum(processing_times)
            if project_processing_time > self.limit and len(project_groups) > 1:
                projects_exceeding_time_limit.append(project_id)
        return projects_exceeding_time_limit
