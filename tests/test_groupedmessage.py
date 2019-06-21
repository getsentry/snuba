import pytz
import simplejson as json
from datetime import datetime

from base import BaseTest
from snuba.datasets.cdc.groupedmessage_processor import GroupedMessageProcessor


class TestGroupedMessageProcessor(BaseTest):

    BEGIN_MSG = '{"event":"begin","xid":2380836}'
    COMMIT_MSG = '{"event":"commit"}'
    UPDATE_MSG = (
        '{"event":"change","xid":2380866,"kind":"update","schema":"public",'
        '"table":"sentry_groupedmessage","columnnames":["id","logger","level","message",'
        '"view","status","times_seen","last_seen","first_seen","data","score","project_id",'
        '"time_spent_total","time_spent_count","resolved_at","active_at","is_public","platform",'
        '"num_comments","first_release_id","short_id"],"columntypes":["bigint","character varying(64)",'
        '"integer","text","character varying(200)","integer","integer","timestamp with time zone",'
        '"timestamp with time zone","text","integer","bigint","integer","integer",'
        '"timestamp with time zone","timestamp with time zone","boolean","character varying(64)","integer",'
        '"bigint","bigint"],"columnvalues":[74,"",40,'
        '"<module> ZeroDivisionError integer division or modulo by zero client3.py __main__ in <module>",'
        '"__main__ in <module>",0,2,"2019-06-19 06:46:28+00","2019-06-19 06:45:32+00",'
        '"eJyT7tuwzAM3PkV2pzJiO34VRSdmvxAgA5dCtViDAGyJEi0AffrSxrZOlSTjrzj3Z1MrOBekCWHBcQaPj4xhXe72WyDv6YU0ouynnDGpMxzrEJSSzCrC+p7Vz8sgNhAvhdOZ/pKOKHd0PC5C9yqtjuPddcPQ9n0w8hPiLRHsWvZGsWD/91xI'
        'ya2IFxz7vJWfTUlHHnwSCEBUkbTZrxCCcOf2baY/XTU1VJm9cjHL4JriHPYvOnliyP0Jt2q4SpLkz7v6owW9E9rEOvl0PawczxcvkLIWppxg==",'
        '1560926969,2,0,0,null,"2019-06-19 06:45:32+00",false,"python",0,null,20],'
        '"oldkeys":{"keynames":["id"],"keytypes":["bigint"],"keyvalues":[74]}}'
    )

    INSERT_MSG = (
        '{"event":"change","xid":2380866,"kind":"insert","schema":"public",'
        '"table":"sentry_groupedmessage","columnnames":["id","logger","level","message",'
        '"view","status","times_seen","last_seen","first_seen","data","score","project_id",'
        '"time_spent_total","time_spent_count","resolved_at","active_at","is_public","platform",'
        '"num_comments","first_release_id","short_id"],"columntypes":["bigint","character varying(64)",'
        '"integer","text","character varying(200)","integer","integer","timestamp with time zone",'
        '"timestamp with time zone","text","integer","bigint","integer","integer",'
        '"timestamp with time zone","timestamp with time zone","boolean","character varying(64)","integer",'
        '"bigint","bigint"],"columnvalues":[74,"",40,'
        '"<module> ZeroDivisionError integer division or modulo by zero client3.py __main__ in <module>",'
        '"__main__ in <module>",0,2,"2019-06-19 06:46:28+00","2019-06-19 06:45:32+00",'
        '"eJyT7tuwzAM3PkV2pzJiO34VRSdmvxAgA5dCtViDAGyJEi0AffrSxrZOlSTjrzj3Z1MrOBekCWHBcQaPj4xhXe72WyDv6YU0ouynnDGpMxzrEJSSzCrC+p7Vz8sgNhAvhdOZ/pKOKHd0PC5C9yqtjuPddcPQ9n0w8hPiLRHsWvZGsWD/91xI'
        'ya2IFxz7vJWfTUlHHnwSCEBUkbTZrxCCcOf2baY/XTU1VJm9cjHL4JriHPYvOnliyP0Jt2q4SpLkz7v6owW9E9rEOvl0PawczxcvkLIWppxg==",'
        '1560926969,2,0,0,null,"2019-06-19 06:45:32+00",false,"python",0,null,20]'
        '}'
    )

    PROCESSED = {
        'commit_id': 2380866,
        'id': 74,
        'logger': '',
        'level': 40,
        'message': '<module> ZeroDivisionError integer division or modulo by zero client3.py __main__ in <module>',
        'view': '__main__ in <module>',
        'status': 0,
        'last_seen': datetime(2019, 6, 19, 6, 46, 28, tzinfo=pytz.UTC),
        'first_seen': datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
        'project_id': 2,
        'active_at': datetime(2019, 6, 19, 6, 45, 32, tzinfo=pytz.UTC),
        'platform': 'python',
        'first_release_id': None,
    }

    def test_messages(self):
        processor = GroupedMessageProcessor()

        begin_msg = json.loads(self.BEGIN_MSG)
        ret = processor.process_message(begin_msg)
        assert ret is None

        commit_msg = json.loads(self.COMMIT_MSG)
        ret = processor.process_message(commit_msg)
        assert ret is None

        insert_msg = json.loads(self.INSERT_MSG)
        ret = processor.process_message(insert_msg)
        assert ret[1] == self.PROCESSED

        update_msg = json.loads(self.UPDATE_MSG)
        ret = processor.process_message(update_msg)
        assert ret[1] == self.PROCESSED
