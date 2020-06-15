import pytz
import simplejson as json
from datetime import datetime

from tests.base import BaseDatasetTest
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.storages.groupassignees import POSTGRES_TABLE
from snuba.utils.streams.kafka import Headers, KafkaPayload
from snuba.utils.streams.types import Message, Partition, Topic


class TestGroupassignee(BaseDatasetTest):
    def setup_method(self, test_method):
        super().setup_method(
            test_method, "groupassignee",
        )

    BEGIN_MSG = '{"event":"begin","xid":2380836}'
    COMMIT_MSG = '{"event":"commit"}'
    UPDATE_MSG_NO_KEY_CHANGE = (
        '{"event":"change","xid":3803891,"timestamp":"2019-09-19 00:06:56.376853+00","kind":"update","schema":"public","table":'
        '"sentry_groupasignee","columnnames":["id","project_id","group_id","user_id","date_added","team_id"],"columntypes":["bigint",'
        '"bigint","bigint","integer","timestamp with time zone","bigint"],"columnvalues":[35,2,1359,1,"2019-09-19 00:17:55+00"'
        ',null],"oldkeys":{"keynames":["project_id", "group_id"],"keytypes":["bigint", "bigint"],"keyvalues":[2, 1359]}}'
    )

    UPDATE_MSG_WITH_KEY_CHANGE = (
        '{"event":"change","xid":3803891,"timestamp":"2019-09-19 00:06:56.376853+00","kind":"update","schema":"public","table":'
        '"sentry_groupasignee","columnnames":["id","project_id","group_id","user_id","date_added","team_id"],"columntypes":["bigint",'
        '"bigint","bigint","integer","timestamp with time zone","bigint"],"columnvalues":[35,3,1359,1,"2019-09-19 00:17:55+00"'
        ',null],"oldkeys":{"keynames":["project_id", "group_id"],"keytypes":["bigint", "bigint"],"keyvalues":[2, 1359]}}'
    )

    DELETE_MSG = (
        '{"event":"change","xid":3803954,"timestamp":"2019-09-19 00:17:21.44787+00","kind":"delete","schema":"public","table":"sent'
        'ry_groupasignee","oldkeys":{"keynames":["project_id", "group_id"],"keytypes":["bigint", "bigint"],"keyvalues":[2, 1359]}}'
    )

    INSERT_MSG = (
        '{"event":"change","xid":3803982,"timestamp":"2019-09-19 00:17:55.032443+00","kind":"insert","schema":"public","table":"sen'
        'try_groupasignee","columnnames":["id","project_id","group_id","user_id","date_added","team_id"],"columntypes":["bigint","b'
        'igint","bigint","integer","timestamp with time zone","bigint"],"columnvalues":[35,2,1359,1,"2019-09-19 00:17:55+00"'
        ",null]}"
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

    def __make_msg(
        self, partition: int, offset: int, payload: str, headers: Headers
    ) -> Message[KafkaPayload]:
        return Message(
            partition=Partition(Topic("topic"), partition),
            offset=offset,
            payload=KafkaPayload(b"key", payload.encode(), headers),
            timestamp=datetime(2019, 6, 19, 6, 46, 28),
        )

    def test_messages(self):
        processor = GroupAssigneeProcessor("sentry_groupasignee")
        message_filter = CdcTableNameMessageFilter(postgres_table=POSTGRES_TABLE)

        metadata = KafkaMessageMetadata(
            offset=42, partition=0, timestamp=datetime(1970, 1, 1)
        )

        assert not message_filter.should_drop(
            self.__make_msg(0, 42, self.BEGIN_MSG, [])
        )
        begin_msg = json.loads(self.BEGIN_MSG)
        ret = processor.process_message(begin_msg, metadata)
        assert ret is None

        assert not message_filter.should_drop(
            self.__make_msg(0, 42, self.COMMIT_MSG, [])
        )
        commit_msg = json.loads(self.COMMIT_MSG)
        ret = processor.process_message(commit_msg, metadata)
        assert ret is None

        assert not message_filter.should_drop(
            self.__make_msg(
                0, 42, self.INSERT_MSG, [("table", POSTGRES_TABLE.encode())]
            )
        )
        insert_msg = json.loads(self.INSERT_MSG)
        ret = processor.process_message(insert_msg, metadata)
        assert ret.data == [self.PROCESSED]
        self.write_processed_records(ret.data)
        ret = (
            get_cluster(StorageSetKey.EVENTS)
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT * FROM groupassignee_local;")
        )
        assert ret[0] == (
            42,  # offset
            0,  # deleted
            2,  # project_id
            1359,  # group_id
            datetime(2019, 9, 19, 0, 17, 55),
            1,  # user_id
            None,  # team_id
        )

        assert not message_filter.should_drop(
            self.__make_msg(
                0,
                42,
                self.UPDATE_MSG_NO_KEY_CHANGE,
                [("table", POSTGRES_TABLE.encode())],
            )
        )
        update_msg = json.loads(self.UPDATE_MSG_NO_KEY_CHANGE)
        ret = processor.process_message(update_msg, metadata)
        assert ret.data == [self.PROCESSED]

        # Tests an update with key change which becomes a two inserts:
        # one deletion and the insertion of the new row.
        assert not message_filter.should_drop(
            self.__make_msg(
                0,
                42,
                self.UPDATE_MSG_WITH_KEY_CHANGE,
                [("table", POSTGRES_TABLE.encode())],
            )
        )
        update_msg = json.loads(self.UPDATE_MSG_WITH_KEY_CHANGE)
        ret = processor.process_message(update_msg, metadata)
        assert ret.data == [self.DELETED, self.PROCESSED_UPDATE]

        assert not message_filter.should_drop(
            self.__make_msg(0, 42, self.DELETE_MSG, [])
        )
        delete_msg = json.loads(self.DELETE_MSG)
        ret = processor.process_message(delete_msg, metadata)
        assert ret.data == [self.DELETED]

    def test_bulk_load(self):
        row = GroupAssigneeRow.from_bulk(
            {
                "project_id": "2",
                "group_id": "1359",
                "date_added": "2019-09-19 00:17:55+00",
                "user_id": "1",
                "team_id": "",
            }
        )
        self.write_processed_records(row.to_clickhouse())
        ret = (
            get_cluster(StorageSetKey.EVENTS)
            .get_query_connection(ClickhouseClientSettings.QUERY)
            .execute("SELECT * FROM groupassignee_local;")
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
