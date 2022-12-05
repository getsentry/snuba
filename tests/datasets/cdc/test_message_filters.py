from datetime import datetime

from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic

from snuba.datasets.message_filters import CdcTableNameMessageFilter


def test_table_name_filter() -> None:
    table_name = "table_name"
    message_filter = CdcTableNameMessageFilter(table_name)

    # Messages that math the table should not be dropped.
    assert not message_filter.should_drop(
        Message(
            BrokerValue(
                KafkaPayload(None, b"", [("table", table_name.encode("utf8"))]),
                Partition(Topic("topic"), 0),
                0,
                datetime.now(),
            )
        )
    )

    # Messages without a table should be dropped.
    assert message_filter.should_drop(
        Message(
            BrokerValue(
                KafkaPayload(None, b"", []),
                Partition(Topic("topic"), 0),
                0,
                datetime.now(),
            )
        )
    )

    # Messages from a different table should be dropped.
    assert message_filter.should_drop(
        Message(
            BrokerValue(
                KafkaPayload(None, b"", [("table", b"other_table")]),
                Partition(Topic("topic"), 0),
                0,
                datetime.now(),
            )
        )
    )
