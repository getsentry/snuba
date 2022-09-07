from datetime import datetime, timedelta

from snuba.utils.bucket_timer import Counter

TEST_COUNTER_WINDOW_SIZE = timedelta(minutes=10)


def test_record_time_spent_over_one_minute() -> None:
    counter = Counter("test-consumer-group")
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=5)
    counter.record_time_spent(-1, start_time, end_time)

    bucket = counter.buckets.pop()
    assert bucket.project_id == -1
    assert bucket.minute == datetime(2022, 1, 1, 1, 1)
    assert bucket.processing_time == timedelta(seconds=5)


def test_record_time_spent_over_multiple_minutes() -> None:
    counter = Counter("test-consumer-group")
    start_time = datetime(2022, 1, 1, 1, 1, 30)
    end_time = start_time + timedelta(seconds=70)
    counter.record_time_spent(-1, start_time, end_time)

    bucket = counter.buckets.popleft()
    assert bucket.project_id == -1
    assert bucket.minute == datetime(2022, 1, 1, 1, 1)
    assert bucket.processing_time == timedelta(seconds=30)

    bucket = counter.buckets.popleft()
    assert bucket.project_id == -1
    assert bucket.minute == datetime(2022, 1, 1, 1, 2)
    assert bucket.processing_time == timedelta(seconds=40)


def test_get_projects_exceeding_limit() -> None:
    counter = Counter("test-consumer-group")
    counter.limit = TEST_COUNTER_WINDOW_SIZE * 0.5

    now = datetime.now()
    counter.record_time_spent(1, now - (TEST_COUNTER_WINDOW_SIZE * 0.2), now)
    counter.record_time_spent(2, now - (TEST_COUNTER_WINDOW_SIZE * 0.6), now)
    counter.record_time_spent(3, now - (TEST_COUNTER_WINDOW_SIZE * 0.01), now)

    exceeded_projects = counter.get_projects_exceeding_limit()
    print(exceeded_projects)
    assert len(exceeded_projects) == 1
    assert exceeded_projects[0] == 2
