from datetime import datetime

from streaming_kafka_consumer import Message, Partition, Topic

from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.utils.streams.backends.kafka import KafkaPayload


def test_table_name_filter() -> None:
    table_name = "table_name"
    message_filter = CdcTableNameMessageFilter(table_name)

    # Messages that math the table should not be dropped.
    assert not message_filter.should_drop(
        Message(
            Partition(Topic("topic"), 0),
            0,
            KafkaPayload(None, b"", [("table", table_name.encode("utf8"))]),
            datetime.now(),
        )
    )

    # Messages without a table should be dropped.
    assert message_filter.should_drop(
        Message(
            Partition(Topic("topic"), 0),
            0,
            KafkaPayload(None, b"", []),
            datetime.now(),
        )
    )

    # Messages from a different table should be dropped.
    assert message_filter.should_drop(
        Message(
            Partition(Topic("topic"), 0),
            0,
            KafkaPayload(None, b"", [("table", b"other_table")]),
            datetime.now(),
        )
    )
