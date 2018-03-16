from base import BaseTest

from snuba.processor import key_for_event, row_for_event


class TestProcessor(BaseTest):
    def test_key(self):
        key = key_for_event(self.base_event)

        assert self.base_event['event_id'] in key
        assert str(self.base_event['project_id']) in key

    def test_simple(self):
        row = row_for_event(self.base_event)

        for field in ('event_id', 'project_id', 'message'):
            assert row[field] == self.base_event[field]
        assert isinstance(row['timestamp'], int)
        assert isinstance(row['received'], int)

    def test_unexpected_obj(self):
        self.base_event['message'] = {'what': 'why is this in the message'}

        row = row_for_event(self.base_event)

        assert row['message'] == '{"what": "why is this in the message"}'
