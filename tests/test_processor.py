import time
from datetime import datetime

from base import BaseTest

from snuba.processor import SnubaProcessor


class MockProducer(object):
    def __init__(self):
        self.received = []

    def send(self, topic, key, value):
        self.received.append((topic, key, value))


class TestProcessor(BaseTest):
    def setup_method(self, test_method):
        super(TestProcessor, self).setup_method(test_method)

        self.mock_producer = MockProducer()

    def test(self):
        processor = SnubaProcessor(self.mock_producer)

        event = {
            'event_id': 'x' * 32,
            'primary_hash': 'x' * 16,
            'project_id': 1,
            'message': 'm',
            'platform': 'p',
            'datetime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'data': {
                'received': time.time(),
            }
        }

        processor.process_event(event)

        assert len(self.mock_producer.received) == 1
