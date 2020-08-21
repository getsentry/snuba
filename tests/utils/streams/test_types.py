import pickle
from datetime import datetime

from snuba.utils.streams.types import Message, Partition, Topic


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")


def test_message_pickling() -> None:
    message = Message(Partition(Topic("topic"), 0), 0, b"", datetime.now())
    assert pickle.loads(pickle.dumps(message)) == message
