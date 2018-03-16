from base import BaseTest

from snuba.processor import get_key, process_raw_event


class TestProcessor(BaseTest):
    def test_key(self):
        key = get_key(self.event)

        assert self.event['event_id'] in key
        assert str(self.event['project_id']) in key

    def test_simple(self):
        processed = process_raw_event(self.event)

        for field in ('event_id', 'project_id', 'message'):
            assert processed[field] == self.event[field]
        assert isinstance(processed['timestamp'], int)
        assert isinstance(processed['received'], int)

    def test_unexpected_obj(self):
        self.event['message'] = {'what': 'why is this in the message'}

        processed = process_raw_event(self.event)

        assert processed['message'] == '{"what": "why is this in the message"}'
