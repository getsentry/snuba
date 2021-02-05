from datetime import datetime
from typing import Optional
from uuid import uuid1

import pytest
import pytz

from snuba.consumers.types import KafkaMessageMetadata
from snuba.consumers.snapshot_worker import SnapshotProcessor
from snuba.datasets.cdc.types import InsertEvent
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.snapshots import SnapshotId
from snuba.snapshots.postgres_snapshot import Xid
from snuba.stateful_consumer.control_protocol import TransactionData


def get_insert_event(xid: int) -> InsertEvent:
    return {
        "event": "change",
        "xid": xid,
        "kind": "insert",
        "schema": "public",
        "table": "sentry_groupedmessage",
        "columnnames": [
            "id",
            "logger",
            "level",
            "message",
            "view",
            "status",
            "times_seen",
            "last_seen",
            "first_seen",
            "data",
            "score",
            "project_id",
            "time_spent_total",
            "time_spent_count",
            "resolved_at",
            "active_at",
            "is_public",
            "platform",
            "num_comments",
            "first_release_id",
            "short_id",
        ],
        "columntypes": [
            "bigint",
            "character varying(64)",
            "integer",
            "text",
            "character varying(200)",
            "integer",
            "integer",
            "timestamp with time zone",
            "timestamp with time zone",
            "text",
            "integer",
            "bigint",
            "integer",
            "integer",
            "timestamp with time zone",
            "timestamp with time zone",
            "boolean",
            "character varying(64)",
            "integer",
            "bigint",
            "bigint",
        ],
        "columnvalues": [
            74,
            "",
            40,
            "<module> ZeroDivisionError integer division or modulo by zero client3.py __main__ in <module>",
            "__main__ in <module>",
            0,
            2,
            "2019-06-19 06:46:28+00",
            "2019-06-19 06:45:32+00",
            "eJyT7tuwzAM3PkV2pzJiO34VRSdmvxAgA5dCtViDAGyJEi0AffrSxrZOlSTjrzj3Z1MrOBekCWHBcQaPj4xhXe72WyDv6YU0ouynnDGpMxzrEJSSzCrC+p7Vz8sgNhAvhdOZ/pKOKHd0PC5C9yqtjuPddcPQ9n0w8hPiLRHsWvZGsWD/91xIya2IFxz7vJWfTUlHHnwSCEBUkbTZrxCCcOf2baY/XTU1VJm9cjHL4JriHPYvOnliyP0Jt2q4SpLkz7v6owW9E9rEOvl0PawczxcvkLIWppxg==",
            1560926969,
            2,
            0,
            0,
            None,
            "2019-06-19 06:45:32+00",
            False,
            "python",
            0,
            None,
            20,
        ],
    }


PROCESSED = {
    "offset": 1,
    "project_id": 2,
    "id": 74,
    "record_deleted": 0,
    "status": 0,
    "last_seen": datetime(2019, 6, 19, 6, 46, 28, tzinfo=pytz.UTC),
    "first_seen": datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
    "active_at": datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
    "first_release_id": None,
}

test_data = [
    (90, None),
    (100, None),
    (110, None),
    (120, InsertBatch([PROCESSED])),
    (210, InsertBatch([PROCESSED])),
]


@pytest.mark.parametrize("xid, expected", test_data)
def test_send_message(xid: int, expected: Optional[ProcessedMessage]) -> None:
    processor = (
        get_writable_storage(StorageKey.GROUPEDMESSAGES)
        .get_table_writer()
        .get_stream_loader()
        .get_processor()
    )

    worker = SnapshotProcessor(
        processor=processor,
        snapshot_id=SnapshotId(str(uuid1())),
        transaction_data=TransactionData(
            xmin=Xid(100), xmax=Xid(200), xip_list=[Xid(120), Xid(130)]
        ),
    )

    ret = worker.process_message(
        get_insert_event(xid),
        KafkaMessageMetadata(offset=1, partition=0, timestamp=datetime.now()),
    )

    assert ret == expected
