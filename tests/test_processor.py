import time
from datetime import datetime

from base import BaseTest

from snuba.processor import SnubaProcessor


class TestProcessor(BaseTest):
    def test(self):
        class MockProducer(object):
            def __init__(self):
                self.received = []

            def send(self, topic, key, value):
                self.received.append((topic, key, value))

        mock_producer = MockProducer()
        processor = SnubaProcessor(mock_producer)

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

        assert len(mock_producer.received) == 1
