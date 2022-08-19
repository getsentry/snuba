from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from snuba.utils.bucket_timer import Counter, compare_counters_and_write_metric
from snuba.utils.metrics.wrapper import MetricsWrapper


def test_write_to_bucket_over_one_minute() -> None:
    counter = Counter()
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=5)
    counter.write_to_bucket(None, start_time, end_time)

    bucket = counter.buckets.pop()
    assert bucket.project_id is None
    assert bucket.minute == datetime(2022, 1, 1, 1, 1)
    assert bucket.processing_time == timedelta(seconds=5)


def test_write_to_bucket_over_multiple_minutes() -> None:
    counter = Counter()
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=70)
    counter.write_to_bucket(None, start_time, end_time)

    # counter.print_buckets()
    bucket = counter.buckets.popleft()
    assert bucket.project_id is None
    assert bucket.minute == datetime(2022, 1, 1, 1, 1)
    assert bucket.processing_time == timedelta(seconds=30)

    bucket = counter.buckets.popleft()
    assert bucket.project_id is None
    assert bucket.minute == datetime(2022, 1, 1, 1, 2)
    assert bucket.processing_time == timedelta(seconds=40)


@patch.object(MetricsWrapper, "increment")
def test_compare_counters_and_write_metric(increment_method_mock: MagicMock) -> None:
    global_counter = Counter()
    project_counter = Counter()

    now = datetime.now()
    project_counter.write_to_bucket(1, now - timedelta(seconds=100), now)
    project_counter.write_to_bucket(2, now - timedelta(seconds=100), now)
    project_counter.write_to_bucket(3, now - timedelta(seconds=10), now)
    global_counter.write_to_bucket(None, now - timedelta(seconds=120), now)

    compare_counters_and_write_metric(global_counter, project_counter, "not available")
    assert increment_method_mock.call_count == 2
    increment_method_mock.assert_any_call(
        "project_exceeded_50_percent_global_processing_time",
        tags={"project_id": str(2)},
    )
    increment_method_mock.assert_any_call(
        "project_exceeded_50_percent_global_processing_time",
        tags={"project_id": str(1)},
    )
