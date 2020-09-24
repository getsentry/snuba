from datetime import datetime

import pytz
import simplejson as json

from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.cdc.groupedmessage_processor import (
    GroupedMessageProcessor,
    GroupedMessageRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.cdc.types import BeginEvent, CommitEvent, DeleteEvent
from snuba.datasets.cdc.types import Event as CDCEvent
from snuba.datasets.cdc.types import InsertEvent, UpdateEvent
from snuba.datasets.storages.groupedmessages import POSTGRES_TABLE
from snuba.processor import InsertBatch
from snuba.utils.streams import Message, Partition, Topic
from snuba.utils.streams.backends.kafka import Headers, KafkaPayload
from tests.base import BaseDatasetTest


class TestGroupedMessage(BaseDatasetTest):
    def setup_method(self, test_method):
        super(TestGroupedMessage, self).setup_method(
            test_method, "groupedmessage",
        )

    BEGIN_MSG = BeginEvent({"event": "begin", "xid": 2380836})
    COMMIT_MSG = CommitEvent({"event": "commit"})
    UPDATE_MSG = UpdateEvent(
        {
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
                "<module> ZeroDivisionError integer division or modulo by "
                "zero client3.py __main__ in <module>",
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
            "event": "change",
            "kind": "update",
            "oldkeys": {"keynames": ["id"], "keytypes": ["bigint"], "keyvalues": [74]},
            "schema": "public",
            "table": "sentry_groupedmessage",
            "xid": 2380866,
        }
    )

    DELETE_MSG = DeleteEvent(
        {
            "event": "change",
            "kind": "delete",
            "oldkeys": {
                "keynames": ["id", "project_id"],
                "keytypes": ["bigint", "bigint"],
                "keyvalues": [74, 2],
            },
            "schema": "public",
            "table": "sentry_groupedmessage",
            "xid": 2380866,
        }
    )

    INSERT_MSG = InsertEvent(
        {
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
                "<module> ZeroDivisionError integer division or modulo by "
                "zero client3.py __main__ in <module>",
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
            "event": "change",
            "kind": "insert",
            "schema": "public",
            "table": "sentry_groupedmessage",
            "xid": 2380866,
        }
    )

    PROCESSED = {
        "offset": 42,
        "project_id": 2,
        "id": 74,
        "record_deleted": 0,
        "status": 0,
        "last_seen": datetime(2019, 6, 19, 6, 46, 28, tzinfo=pytz.UTC),
        "first_seen": datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
        "active_at": datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
        "first_release_id": None,
    }

    DELETED = {
        "offset": 42,
        "project_id": 2,
        "id": 74,
        "record_deleted": 1,
        "status": None,
        "last_seen": None,
        "first_seen": None,
        "active_at": None,
        "first_release_id": None,
    }

    def __make_msg(
        self, partition: int, offset: int, payload: CDCEvent, headers: Headers
    ) -> Message[KafkaPayload]:
        return Message(
            partition=Partition(Topic("topic"), partition),
            offset=offset,
            payload=KafkaPayload(b"key", json.dumps(payload).encode(), headers),
            timestamp=datetime(2019, 6, 19, 6, 46, 28),
        )

    def test_messages(self) -> None:
        processor = GroupedMessageProcessor("sentry_groupedmessage")
        message_filter = CdcTableNameMessageFilter(postgres_table=POSTGRES_TABLE)

        metadata = KafkaMessageMetadata(
            offset=42, partition=0, timestamp=datetime(1970, 1, 1)
        )

        assert message_filter.should_drop(self.__make_msg(0, 42, self.BEGIN_MSG, []))

        assert message_filter.should_drop(self.__make_msg(0, 42, self.COMMIT_MSG, []))

        assert not message_filter.should_drop(
            self.__make_msg(
                0, 42, self.INSERT_MSG, [("table", "sentry_groupedmessage".encode())]
            )
        )
        ret = processor.process_message(self.INSERT_MSG, metadata)
        assert ret == InsertBatch([self.PROCESSED])
        self.write_processed_messages([ret])
        ret = (
            get_cluster(StorageSetKey.EVENTS)
            .get_query_connection(ClickhouseClientSettings.INSERT)
            .execute("SELECT * FROM groupedmessage_local;")
        )
        assert ret[0] == (
            42,  # offset
            0,  # deleted
            2,  # project_id
            74,  # id
            0,  # status
            datetime(2019, 6, 19, 6, 46, 28),
            datetime(2019, 6, 19, 6, 45, 32),
            datetime(2019, 6, 19, 6, 45, 32),
            None,
        )

        assert not message_filter.should_drop(
            self.__make_msg(
                0, 42, self.UPDATE_MSG, [("table", "sentry_groupedmessage".encode())]
            )
        )
        ret = processor.process_message(self.UPDATE_MSG, metadata)
        assert ret == InsertBatch([self.PROCESSED])

        assert not message_filter.should_drop(
            self.__make_msg(
                0, 42, self.DELETE_MSG, [("table", "sentry_groupedmessage".encode())]
            )
        )
        ret = processor.process_message(self.DELETE_MSG, metadata)
        assert ret == InsertBatch([self.DELETED])

    def test_ignored_table(self) -> None:
        message_filter = CdcTableNameMessageFilter(postgres_table=POSTGRES_TABLE)

        assert message_filter.should_drop(
            self.__make_msg(
                0, 42, self.UPDATE_MSG, [("table", "NOT_groupedmessage".encode())]
            )
        )

    def test_bulk_load(self) -> None:
        row = GroupedMessageRow.from_bulk(
            {
                "project_id": "2",
                "id": "10",
                "status": "0",
                "last_seen": "2019-06-28 17:57:32+00",
                "first_seen": "2019-06-28 06:40:17+00",
                "active_at": "2019-06-28 06:40:17+00",
                "first_release_id": "26",
            }
        )
        self.write_rows([row.to_clickhouse()])
        ret = (
            get_cluster(StorageSetKey.EVENTS)
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT * FROM groupedmessage_local;")
        )
        assert ret[0] == (
            0,  # offset
            0,  # deleted
            2,  # project_id
            10,  # id
            0,  # status
            datetime(2019, 6, 28, 17, 57, 32),
            datetime(2019, 6, 28, 6, 40, 17),
            datetime(2019, 6, 28, 6, 40, 17),
            26,
        )
