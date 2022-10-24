from datetime import datetime

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload

from snuba.datasets.message_filters import KafkaHeaderSelectFilter

test_data = [
    pytest.param(
        KafkaHeaderSelectFilter("should_drop", "1"),
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
        KafkaHeaderSelectFilter("should_drop", "0"),
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
        KafkaHeaderSelectFilter("should_drop", "1"),
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


@pytest.mark.parametrize("select_filter, message, expected_drop_result", test_data)
def test_kafka_filter_header_should_drop(
    select_filter: KafkaHeaderSelectFilter,
    message: Message[KafkaPayload],
    expected_drop_result: bool,
) -> None:
    assert select_filter.should_drop(message) == (not expected_drop_result)
