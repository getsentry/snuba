from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Deque, List, Optional

from snuba import environment
from snuba.utils.metrics.wrapper import MetricsWrapper

WINDOW_SIZE = timedelta(minutes=10)  # minutes


class Bucket:
    def __init__(
        self,
        project_id: Optional[int],
        minute: datetime,
        processing_time: timedelta = timedelta(seconds=0),
    ) -> None:
        self.project_id = project_id  # is None for global bucket
        self.minute = minute
        self.processing_time = processing_time

    def add_processing_time_seconds(self, seconds: timedelta) -> None:
        self.processing_time += seconds


class Counter:
    def __init__(self, consumer_group: str) -> None:
        self.consumer_group: str = consumer_group
        self.buckets: Deque[Bucket] = deque()

    def get_time_sum(self, group: List[timedelta]) -> timedelta:
        return sum([processing_time for processing_time in group], timedelta())

    def floor_minute(self, time: datetime) -> datetime:
        return time - timedelta(seconds=time.second, microseconds=time.microsecond)

    def ceil_minute(self, time: datetime) -> datetime:
        if time.second == 0 and time.microsecond == 0:
            return time
        return self.floor_minute(time + timedelta(minutes=1))

    def trim_expired_buckets(self, now: datetime) -> None:
        current_minute = self.floor_minute(now)
        window = current_minute - WINDOW_SIZE
        while self.buckets and self.buckets[0].minute < window:
            self.buckets.popleft()

    def get_existing_bucket(
        self, project_id: Optional[int], minute: datetime
    ) -> Optional[Bucket]:
        for bucket in self.buckets:
            if bucket.project_id == project_id and bucket.minute == minute:
                return bucket
        return None

    def create_and_add_bucket(
        self,
        project_id: Optional[int],
        start_minute: datetime,
        processing_time: timedelta,
    ) -> None:
        bucket = self.get_existing_bucket(project_id, start_minute)
        if not bucket:
            bucket = Bucket(project_id, start_minute, processing_time)
            self.buckets.append(bucket)
        else:
            bucket.add_processing_time_seconds(processing_time)

    def write_to_bucket(
        self, project_id: Optional[int], start: datetime, end: datetime
    ) -> None:
        start_minute = self.floor_minute(start)
        left = start
        right = self.ceil_minute(start)
        while right <= end:
            self.create_and_add_bucket(project_id, start_minute, right - left)
            left = start_minute = right
            right += timedelta(minutes=1)
        self.create_and_add_bucket(project_id, start_minute, end - left)

    def print_buckets(self) -> None:
        for bucket in self.buckets:
            print(bucket.minute, bucket.project_id, bucket.processing_time)

    def compare_and_write_metric(self) -> None:
        metrics = MetricsWrapper(
            environment.metrics, "replacer", tags={"group": self.consumer_group}
        )
        now = datetime.now()
        self.trim_expired_buckets(now)
        project_groups = defaultdict(list)
        for bucket in self.buckets:
            project_groups[bucket.project_id].append(bucket.processing_time)

        # Compare the replacement total grouped by project_id with system time
        for project_id, processing_times in project_groups.items():
            project_processing_time = self.get_time_sum(processing_times)
            if project_processing_time > WINDOW_SIZE * 0.5 and len(project_groups) > 1:
                metrics.increment(
                    "project_processing_time_exceeded_time_interval",
                    tags={"project_id": str(project_id)},
                )
