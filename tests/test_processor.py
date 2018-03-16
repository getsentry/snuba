import time
from datetime import datetime

from base import BaseTest

from snuba.processor import SnubaProcessor


class TestProcessor(BaseTest):
    def test(self):
        processor = SnubaProcessor()

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

        key, value = processor.process_event(event)
