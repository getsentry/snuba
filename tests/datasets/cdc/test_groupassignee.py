from datetime import datetime

import pytest
import pytz

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.cdc.types import DeleteEvent, InsertEvent, UpdateEvent
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch
from tests.helpers import write_processed_messages


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestGroupassignee:
    storage = get_writable_storage(StorageKey.GROUPASSIGNEES)

    UPDATE_MSG_NO_KEY_CHANGE = UpdateEvent(
        {
            "columnnames": [
                "id",
                "project_id",
                "group_id",
                "user_id",
                "date_added",
                "team_id",
            ],
            "columntypes": [
                "bigint",
                "bigint",
                "bigint",
                "integer",
                "timestamp with time zone",
                "bigint",
            ],
            "columnvalues": [35, 2, 1359, 1, "2019-09-19 00:17:55+00", None],
            "event": "change",
            "kind": "update",
            "oldkeys": {
                "keynames": ["project_id", "group_id"],
                "keytypes": ["bigint", "bigint"],
                "keyvalues": [2, 1359],
            },
            "schema": "public",
            "table": "sentry_groupasignee",
            "timestamp": "2019-09-19 00:06:56.376853+00",
            "xid": 3803891,
        }
    )
    UPDATE_MSG_WITH_KEY_CHANGE = UpdateEvent(
        {
            "columnnames": [
                "id",
                "project_id",
                "group_id",
                "user_id",
                "date_added",
                "team_id",
            ],
            "columntypes": [
                "bigint",
                "bigint",
                "bigint",
                "integer",
                "timestamp with time zone",
                "bigint",
            ],
            "columnvalues": [35, 3, 1359, 1, "2019-09-19 00:17:55+00", None],
            "event": "change",
            "kind": "update",
            "oldkeys": {
                "keynames": ["project_id", "group_id"],
                "keytypes": ["bigint", "bigint"],
                "keyvalues": [2, 1359],
            },
            "schema": "public",
            "table": "sentry_groupasignee",
            "timestamp": "2019-09-19 00:06:56.376853+00",
            "xid": 3803891,
        }
    )

    DELETE_MSG = DeleteEvent(
        {
            "event": "change",
            "kind": "delete",
            "oldkeys": {
                "keynames": ["project_id", "group_id"],
                "keytypes": ["bigint", "bigint"],
                "keyvalues": [2, 1359],
            },
            "schema": "public",
            "table": "sentry_groupasignee",
            "timestamp": "2019-09-19 00:17:21.44787+00",
            "xid": 3803954,
        }
    )

    INSERT_MSG = InsertEvent(
        {
            "columnnames": [
                "id",
                "project_id",
                "group_id",
                "user_id",
                "date_added",
                "team_id",
            ],
            "columntypes": [
                "bigint",
                "bigint",
                "bigint",
                "integer",
                "timestamp with time zone",
                "bigint",
            ],
            "columnvalues": [35, 2, 1359, 1, "2019-09-19 00:17:55+00", None],
            "event": "change",
            "kind": "insert",
            "schema": "public",
            "table": "sentry_groupasignee",
            "timestamp": "2019-09-19 00:17:55.032443+00",
            "xid": 3803982,
        }
    )

    PROCESSED = {
        "offset": 42,
        "project_id": 2,
        "group_id": 1359,
        "record_deleted": 0,
        "user_id": 1,
        "team_id": None,
        "date_added": datetime(2019, 9, 19, 0, 17, 55, tzinfo=pytz.UTC),
    }

    PROCESSED_UPDATE = {
        "offset": 42,
        "project_id": 3,
        "group_id": 1359,
        "record_deleted": 0,
        "user_id": 1,
        "team_id": None,
        "date_added": datetime(2019, 9, 19, 0, 17, 55, tzinfo=pytz.UTC),
    }

    DELETED = {
        "offset": 42,
        "project_id": 2,
        "group_id": 1359,
        "record_deleted": 1,
        "user_id": None,
        "team_id": None,
        "date_added": None,
    }

    def test_messages(self) -> None:
        processor = GroupAssigneeProcessor("sentry_groupasignee")

        metadata = KafkaMessageMetadata(
            offset=42, partition=0, timestamp=datetime(1970, 1, 1)
        )

        ret = processor.process_message(self.INSERT_MSG, metadata)
        assert ret == InsertBatch(
            [self.PROCESSED], datetime(2019, 9, 19, 0, 17, 55, 32443, tzinfo=pytz.UTC)
        )
        write_processed_messages(self.storage, [ret])
        results = (
            self.storage.get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT * FROM groupassignee_local;")
            .results
        )
        assert results[0] == (
            42,  # offset
            0,  # deleted
            2,  # project_id
            1359,  # group_id
            datetime(2019, 9, 19, 0, 17, 55),
            1,  # user_id
            None,  # team_id
        )

        ret = processor.process_message(self.UPDATE_MSG_NO_KEY_CHANGE, metadata)
        assert ret == InsertBatch(
            [self.PROCESSED], datetime(2019, 9, 19, 0, 6, 56, 376853, tzinfo=pytz.UTC)
        )

        # Tests an update with key change which becomes a two inserts:
        # one deletion and the insertion of the new row.
        ret = processor.process_message(self.UPDATE_MSG_WITH_KEY_CHANGE, metadata)
        assert ret == InsertBatch(
            [self.DELETED, self.PROCESSED_UPDATE],
            datetime(2019, 9, 19, 0, 6, 56, 376853, tzinfo=pytz.UTC),
        )

        ret = processor.process_message(self.DELETE_MSG, metadata)
        assert ret == InsertBatch(
            [self.DELETED], datetime(2019, 9, 19, 0, 17, 21, 447870, tzinfo=pytz.UTC)
        )

    def test_bulk_load(self) -> None:
        row = GroupAssigneeRow.from_bulk(
            {
                "project_id": "2",
                "group_id": "1359",
                "date_added": "2019-09-19 00:17:55+00",
                "user_id": "1",
                "team_id": "",
            }
        )
        write_processed_messages(
            self.storage, [InsertBatch([row.to_clickhouse()], None)]
        )
        ret = (
            self.storage.get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT * FROM groupassignee_local;")
            .results
        )
        assert ret[0] == (
            0,  # offset
            0,  # deleted
            2,  # project_id
            1359,  # group_id
            datetime(2019, 9, 19, 0, 17, 55),
            1,  # user_id
            None,  # team_id
        )
