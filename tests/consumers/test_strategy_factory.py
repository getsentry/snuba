from datetime import datetime, timedelta
from typing import Any
from unittest.mock import Mock, patch

from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.consumers.strategy_factory import ProduceCommitLog


@patch("time.time")
def test_produce_commit_log(time_mock: Any) -> None:
    start = datetime(2022, 12, 12)
    time_mock.return_value = start.timestamp()

    producer = Mock()
    main_topic = Topic("main-topic")
    partition = Partition(main_topic, 0)
    now = datetime.now()
    commit_log_topic = Topic("commit-log")
    commit_func = Mock()
    strategy = ProduceCommitLog(
        producer, commit_log_topic, "errors-consumer-group", commit_func
    )

    strategy.submit(Message(BrokerValue(0, partition, 1, now)))

    assert commit_func.call_count == 0
    assert producer.produce.call_count == 0

    # Advance time by 1 second
    now = start + timedelta(seconds=1)
    time_mock.return_value = now.timestamp()

    strategy.poll()
    assert commit_func.call_count == 1
    assert producer.produce.call_count == 1
