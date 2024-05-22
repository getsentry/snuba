from datetime import datetime, timedelta

from snuba.utils.bucket_timer import Counter, floor_minute
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls

TEST_COUNTER_WINDOW_SIZE = timedelta(minutes=10)


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
    counter = Counter("test-consumer-group")
    counter.limit = TEST_COUNTER_WINDOW_SIZE * 0.5

    now = datetime.now()
    counter.record_time_spent(1, now - (TEST_COUNTER_WINDOW_SIZE * 0.2), now)
    counter.record_time_spent(2, now - (TEST_COUNTER_WINDOW_SIZE * 0.6), now)
    counter.record_time_spent(3, now - (TEST_COUNTER_WINDOW_SIZE * 0.01), now)

    exceeded_projects = counter.get_projects_exceeding_limit()
    assert len(exceeded_projects) == 1
    assert exceeded_projects[0] == 2
    metric_calls = get_recorded_metric_calls(
        "gauge", "bucket_timer.bucket_timer_projects_to_skip"
    )
    assert metric_calls is not None
    assert len(metric_calls) == 1
