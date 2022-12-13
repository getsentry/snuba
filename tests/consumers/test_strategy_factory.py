from datetime import datetime
from unittest.mock import Mock

from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.consumers.strategy_factory import ProduceCommitLog


def test_produce_commit_log() -> None:
    now = datetime.now()
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

    assert commit_func.call_count == 1
    assert producer.produce.call_count == 1
