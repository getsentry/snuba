from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, MutableMapping

from snuba import environment, settings, state
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "bucket_timer")


def floor_minute(time: datetime) -> datetime:
    return time - timedelta(seconds=time.second, microseconds=time.microsecond)


def ceil_minute(time: datetime) -> datetime:
    if time.second == 0 and time.microsecond == 0:
        return time
    return floor_minute(time + timedelta(minutes=1))


Buckets = MutableMapping[datetime, MutableMapping[int, timedelta]]

COUNTER_WINDOW_SIZE = timedelta(minutes=settings.COUNTER_WINDOW_SIZE_MINUTES)


class Counter:
    """
    The Counter class is used to track time spent on some activity (e.g. processing a replacement) for a project.
    To accomplish this, the `record_time_spent()` function captures some processing time range and splits it by a per
    minute resolution (Bucket). The buckets older than COUNTER_WINDOW_SIZE are trimmed. Finally, the `get_bucket_totals_exceeding_limit()`
    function returns all project ids who's total processing time has exceeded self.limit.
    """

    def __init__(self, consumer_group: str) -> None:
        self.consumer_group: str = consumer_group
        self.buckets: Buckets = {}

        percentage = state.get_config("project_quota_time_percentage", 1.0)
        assert isinstance(percentage, float)
        self.limit = COUNTER_WINDOW_SIZE * percentage

    def __trim_expired_buckets(self, now: datetime) -> None:
        current_minute = floor_minute(now)
        window_start = current_minute - COUNTER_WINDOW_SIZE
        new_buckets: Buckets = {}
        for min, dict in self.buckets.items():
            if min >= window_start:
                new_buckets[min] = dict
        self.buckets = new_buckets

    def __add_to_bucket(
        self,
        project_id: int,
        start_minute: datetime,
        processing_time: timedelta,
    ) -> None:
        if start_minute in self.buckets:
            if project_id in self.buckets[start_minute]:
                self.buckets[start_minute][project_id] += processing_time
            else:
                self.buckets[start_minute][project_id] = processing_time
        else:
            self.buckets[start_minute] = {}
            self.buckets[start_minute][project_id] = processing_time

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
        project_groups: dict[int, timedelta] = defaultdict(lambda: timedelta(seconds=0))
        for project_dict in list(self.buckets.values()):
            for project_id, processing_time in project_dict.items():
                project_groups[project_id] += processing_time

        # Compare the replacement total grouped by project_id with system time limit
        projects_exceeding_time_limit = []
        for project_id, total_processing_time in project_groups.items():
            if total_processing_time > self.limit and len(project_groups) > 1:
                projects_exceeding_time_limit.append(project_id)
        metrics.timing(
            "get_projects_exceeding_limit_duration",
            datetime.now().timestamp() - now.timestamp(),
            tags={},
        )
        return projects_exceeding_time_limit
