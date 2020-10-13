import logging
import json
from datetime import datetime
from snuba.utils.streams import Message, Topic, Partition
from snuba.utils.streams.backends.kafka import KafkaPayload
from tests.datasets.cdc.test_groupedmessage import TestGroupedMessage
from tests.datasets.cdc.test_groupassignee import TestGroupassignee

logging.basicConfig(level=logging.DEBUG)

data = [
    ("sentry_other", b"{}"),
    ("sentry_groupedmessage", json.dumps(TestGroupedMessage.INSERT_MSG)),
    ("sentry_groupasignee", json.dumps(TestGroupassignee.INSERT_MSG)),
]

messages = [
    Message(
        Partition(Topic("cdc"), 0),
        0,
        KafkaPayload(None, event, [("table", table.encode("utf8"))]),
        datetime.now(),
        1,
    )
    for table, event in data
]

from snuba.datasets.storages import groupassignees
from snuba.datasets.storages import groupedmessages
from snuba.consumer import MultipleStorageConsumerStrategyFactory
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend

s = MultipleStorageConsumerStrategyFactory(
    [groupassignees.storage, groupedmessages.storage], DummyMetricsBackend(), 10, 10
).create(lambda offsets: None)

for m in messages:
    s.submit(m)

s.close()
s.join()
