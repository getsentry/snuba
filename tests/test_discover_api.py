import calendar
from datetime import datetime
from functools import partial
import simplejson as json
import uuid

from snuba import settings
from snuba.datasets.factory import enforce_table_writer, get_dataset

from tests.base import BaseApiTest, get_event


class TestApi(BaseApiTest):
    def setup_method(self, test_method):
        # Setup both tables
        super().setup_method(test_method, 'events')
        super().setup_method(test_method, 'transactions')

        self.app.post = partial(self.app.post, headers={'referer': 'test'})
        self.project_id = 1

        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.generate_event()
        self.generate_transaction()

    def generate_event(self):
        self.dataset = get_dataset('events')
        event = get_event()
        event['project_id'] = self.project_id
        event = enforce_table_writer(self.dataset).get_stream_loader().get_processor().process_insert(event)
        self.write_processed_records([event])

    def generate_transaction(self):
        self.dataset = get_dataset('transactions')

        trace_id = '7400045b25c443b885914600aa83ad04'
        span_id = '8841662216cc598b'
        processed = enforce_table_writer(self.dataset).get_stream_loader().get_processor().process_message(
            (2,
            'insert',
            {
                'project_id': self.project_id,
                'event_id': uuid.uuid4().hex,
                'deleted': 0,
                'datetime': (self.base_time).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                'platform': 'python',
                'retention_days': settings.DEFAULT_RETENTION_DAYS,
                'data': {
                    'received': calendar.timegm((self.base_time).timetuple()),
                    'type': 'transaction',
                    'transaction': '/api/do_things',

                    'start_timestamp': datetime.timestamp(self.base_time),
                    'timestamp': datetime.timestamp(self.base_time),
                    'tags': {
                        # Sentry
                        'environment': u'prød',
                        'sentry:release': '1',
                        'sentry:dist': 'dist1',

                        # User
                        'foo': 'baz',
                        'foo.bar': 'qux',
                        'os_name': 'linux',
                    },
                    'user': {
                        'email': 'sally@example.org',
                        'ip_address': '8.8.8.8',
                    },
                    'contexts': {
                        'trace': {
                            'trace_id': trace_id,
                            'span_id': span_id,
                            'op': 'http',
                        },
                    },
                    'spans': [
                        {
                            'op': 'db',
                            'trace_id': trace_id,
                            'span_id': span_id + '1',
                            'parent_span_id': None,
                            'same_process_as_parent': True,
                            'description': 'SELECT * FROM users',
                            'data': {},
                            'timestamp': calendar.timegm((self.base_time).timetuple()),
                        }
                    ]
                }
            }))

        self.write_processed_events(processed.data)

    def test_type_condition(self):
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'discover',
            'project': self.project_id,
            'selected_columns': ['type', 'tags[custom_tag]'],
            'conditions': [['type', '!=', 'transaction']],
            'orderby': 'timestamp',
            'limit': 1000,
        }))
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data['data']) == 1, data
        assert data['data'][0] == {'type': 'error', 'tags[custom_tag]': 'custom_value'}

        response = self.app.post('/query', data=json.dumps({
            'dataset': 'discover',
            'project': 1,
            'selected_columns': ['type', 'tags[foo]', 'group_id'],
            'conditions': [['type', '=', 'transaction']],
            'orderby': 'timestamp',
            'limit': 1,
        }))
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data['data']) == 1, data
        assert data['data'][0] == {'type': 'transaction', 'tags[foo]': 'baz', 'group_id': None}
