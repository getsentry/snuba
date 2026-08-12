from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any
from unittest import mock

from snuba.utils.bucket_timer import Counter, floor_minute

TEST_COUNTER_WINDOW_SIZE = timedelta(minutes=10)


@contextmanager
def options(**overrides: Any) -> Iterator[None]:
    def get_option(key: str, default: Any) -> Any:
        return overrides.get(key, default)

    with mock.patch("snuba.utils.bucket_timer.get_option", side_effect=get_option):
        yield


def test_record_time_spent_over_one_minute() -> None:
    counter = Counter("test-consumer-group")
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=5)
    counter.record_time_spent(-1, start_time, end_time)

    start_minute = floor_minute(start_time)
    assert start_minute in counter.buckets
    assert -1 in counter.buckets[start_minute]
    assert counter.buckets[start_minute][-1] == timedelta(seconds=5)


def test_record_time_spent_over_multiple_minutes() -> None:
    counter = Counter("test-consumer-group")
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=70)
    counter.record_time_spent(-1, start_time, end_time)

    start_minute = floor_minute(start_time)
    assert start_minute in counter.buckets
    assert -1 in counter.buckets[start_minute]
    assert counter.buckets[start_minute][-1] == timedelta(seconds=30)

    next_minute = floor_minute(end_time)
    assert next_minute in counter.buckets
    assert -1 in counter.buckets[next_minute]
    assert counter.buckets[next_minute][-1] == timedelta(seconds=40)


def test_get_projects_exceeding_limit() -> None:
    with options(project_quota_time_percentage=0.5, counter_window_size_minutes=10):
        counter = Counter("test-consumer-group")

        now = datetime.now()
        counter.record_time_spent(1, now - (TEST_COUNTER_WINDOW_SIZE * 0.2), now)
        counter.record_time_spent(2, now - (TEST_COUNTER_WINDOW_SIZE * 0.6), now)
        counter.record_time_spent(3, now - (TEST_COUNTER_WINDOW_SIZE * 0.01), now)

        exceeded_projects = counter.get_projects_exceeding_limit()

    assert len(exceeded_projects) == 1
    assert exceeded_projects[0] == 2


def test_limit_tracks_options_after_construction() -> None:
    counter = Counter("test-consumer-group")

    now = datetime.now()
    counter.record_time_spent(1, now - (TEST_COUNTER_WINDOW_SIZE * 0.6), now)
    counter.record_time_spent(2, now - (TEST_COUNTER_WINDOW_SIZE * 0.01), now)

    with options(project_quota_time_percentage=1.0, counter_window_size_minutes=10):
        assert counter.get_projects_exceeding_limit() == []

    # Tightening the quota takes effect without rebuilding the Counter.
    with options(project_quota_time_percentage=0.5, counter_window_size_minutes=10):
        assert counter.get_projects_exceeding_limit() == [1]
