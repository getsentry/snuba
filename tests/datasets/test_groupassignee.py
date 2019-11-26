import pytz
import simplejson as json
from datetime import datetime

from tests.base import BaseDatasetTest
from snuba.clickhouse.native import ClickhousePool
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)


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

    def test_messages(self):
        processor = GroupAssigneeProcessor("sentry_groupasignee")

        metadata = KafkaMessageMetadata(offset=42, partition=0,)

        begin_msg = json.loads(self.BEGIN_MSG)
        ret = processor.process_message(begin_msg, metadata)
        assert ret is None

        commit_msg = json.loads(self.COMMIT_MSG)
        ret = processor.process_message(commit_msg, metadata)
        assert ret is None

        insert_msg = json.loads(self.INSERT_MSG)
        ret = processor.process_message(insert_msg, metadata)
        assert ret.data == [self.PROCESSED]
        self.write_processed_records(ret.data)
        cp = ClickhousePool()
        ret = cp.execute("SELECT * FROM test_groupassignee_local;")
        assert ret[0] == (
            42,  # offset
            0,  # deleted
            2,  # project_id
            1359,  # group_id
            datetime(2019, 9, 19, 0, 17, 55),
            1,  # user_id
            None,  # team_id
        )

        update_msg = json.loads(self.UPDATE_MSG_NO_KEY_CHANGE)
        ret = processor.process_message(update_msg, metadata)
        assert ret.data == [self.PROCESSED]

        # Tests an update with key change which becomes a two inserts:
        # one deletion and the insertion of the new row.
        update_msg = json.loads(self.UPDATE_MSG_WITH_KEY_CHANGE)
        ret = processor.process_message(update_msg, metadata)
        assert ret.data == [self.DELETED, self.PROCESSED_UPDATE]

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
        cp = ClickhousePool()
        ret = cp.execute("SELECT * FROM test_groupassignee_local;")
        assert ret[0] == (
            0,  # offset
            0,  # deleted
            2,  # project_id
            1359,  # group_id
            datetime(2019, 9, 19, 0, 17, 55),
            1,  # user_id
            None,  # team_id
        )
