from datetime import datetime

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload

from snuba.datasets.message_filters import (
    KafkaHeaderFilter,
    KafkaHeaderFilterWithBypass,
)

test_data = [
    pytest.param(
        KafkaHeaderFilter("should_drop", "1"),
        Message(
            Partition(Topic("random"), 1),
            1,
            KafkaPayload(b"key", b"value", [("should_drop", b"1")]),
            datetime.now(),
        ),
        True,
        id="matching-headers",
    ),
    pytest.param(
        KafkaHeaderFilter("should_drop", "0"),
        Message(
            Partition(Topic("random"), 1),
            1,
            KafkaPayload(b"key", b"value", [("should_drop", b"1")]),
            datetime.now(),
        ),
        False,
        id="mismatched-headers",
    ),
    pytest.param(
        KafkaHeaderFilter("should_drop", "1"),
        Message(
            Partition(Topic("random"), 1),
            1,
            KafkaPayload(b"key", b"value", [("missing", b"0")]),
            datetime.now(),
        ),
        False,
        id="missing-headers",
    ),
]


@pytest.mark.parametrize("header_filter, message, expected_result", test_data)
def test_kafka_filter_header_should_drop(
    header_filter: KafkaHeaderFilter,
    message: Message[KafkaPayload],
    expected_result: bool,
) -> None:
    assert header_filter.should_drop(message) == expected_result


def test_kafka_filter_header_with_bypass() -> None:
    header_filter = KafkaHeaderFilterWithBypass("should_drop", "1", 5)
    message = Message(
        Partition(Topic("random"), 1),
        1,
        KafkaPayload(b"key", b"value", [("should_drop", b"1")]),
        datetime.now(),
    )

    for _ in range(3):
        assert header_filter.should_drop(message) is True
        assert header_filter.should_drop(message) is True
        assert header_filter.should_drop(message) is True
        assert header_filter.should_drop(message) is True
        assert header_filter.should_drop(message) is False
