from snuba.utils.streams.types import (
    Partition,
    Topic,
)


def test_topic_contains_partition() -> None:
    assert Partition(Topic("topic"), 0) in Topic("topic")
    assert Partition(Topic("topic"), 0) not in Topic("other-topic")
    assert Partition(Topic("other-topic"), 0) not in Topic("topic")
