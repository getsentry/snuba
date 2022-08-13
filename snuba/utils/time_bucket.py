from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Deque, List, Optional

from snuba import environment
from snuba.utils.metrics.wrapper import MetricsWrapper


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
    def __init__(self) -> None:
        self.buckets: Deque[Bucket] = deque()

    def get_time_sum(self, group: List[timedelta]) -> timedelta:
        return sum([processing_time for processing_time in group], timedelta())

    def floor_minute(self, time: datetime) -> datetime:
        return time - timedelta(seconds=time.second, microseconds=time.microsecond)

    def ceil_minute(self, time: datetime) -> datetime:
        if time.second == 0 and time.microsecond == 0:
            return time
        return self.floor_minute(time + timedelta(minutes=1))

    def reorganize_buckets(self, now: datetime) -> None:
        current_minute = self.floor_minute(now)
        five_minutes_ago = current_minute - timedelta(minutes=5)
        while self.buckets and self.buckets[0].minute < five_minutes_ago:
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
            left = right
            right += timedelta(minutes=1)
        self.create_and_add_bucket(project_id, start_minute, end - left)

    def print_buckets(self) -> None:
        for bucket in self.buckets:
            print(bucket.minute, bucket.project_id, bucket.processing_time)


def compare_counters(
    global_consumer_counter: Counter, project_counter: Counter, consumer_group: str
) -> None:
    metrics = MetricsWrapper(
        environment.metrics, "replacer", tags={"group": consumer_group}
    )
    now = datetime.now()
    global_consumer_counter.reorganize_buckets(now)
    project_counter.reorganize_buckets(now)
    project_groups = defaultdict(list)
    for bucket in project_counter.buckets:
        project_groups[bucket.project_id].append(bucket.processing_time)

    for project_id, processing_times in project_groups.items():
        project_total_processing_time = project_counter.get_time_sum(processing_times)
        replacer_total_processing_time = global_consumer_counter.get_time_sum(
            [
                bucket.processing_time
                for bucket in global_consumer_counter.buckets
                if not bucket.project_id
            ]
        )
        if project_total_processing_time > replacer_total_processing_time * 0.5:
            metrics.increment(
                "project_exceeded_50_percent_global_processing_time",
                tags={"project_id": str(project_id)},
            )
