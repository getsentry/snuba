# -*- coding: utf-8 -*-
import calendar
from datetime import datetime, timedelta
from functools import partial
import pytz
import simplejson as json
import uuid

from snuba import settings, state
from snuba.datasets.factory import enforce_table_writer

from tests.base import BaseApiTest


class TestTransactionsApi(BaseApiTest):
    def setup_method(self, test_method, dataset_name='transactions'):
        super().setup_method(test_method, dataset_name)
        self.app.post = partial(self.app.post, headers={'referer': 'test'})

        # values for test data
        self.project_ids = [1, 2]  # 2 projects
        self.environments = [u'prød', 'test']  # 2 environments
        self.platforms = ['a', 'b']  # 2 platforms
        self.hashes = [x * 32 for x in '0123456789ab']  # 12 hashes
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - \
            timedelta(minutes=self.minutes)
        self.generate_fizzbuzz_events()

    def teardown_method(self, test_method):
        # Reset rate limits
        state.delete_config('global_concurrent_limit')
        state.delete_config('global_per_second_limit')
        state.delete_config('project_concurrent_limit')
        state.delete_config('project_concurrent_limit_1')
        state.delete_config('project_per_second_limit')
        state.delete_config('date_align_seconds')

    def generate_fizzbuzz_events(self):
        """
        Generate a deterministic set of events across a time range.
        """
        events = []
        for tick in range(self.minutes):
            tock = tick + 1
            for p in self.project_ids:
                # project N sends an event every Nth minute
                if tock % p == 0:
                    trace_id = '7400045b25c443b885914600aa83ad04'
                    span_id = '8841662216cc598b'
                    processed = enforce_table_writer(self.dataset).get_stream_loader().get_processor().process_message(
                        (2,
                        'insert',
                        {
                            'project_id': p,
                            'event_id': uuid.uuid4().hex,
                            'deleted': 0,
                            'datetime': (self.base_time + timedelta(minutes=tick)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            'platform': self.platforms[(tock * p) % len(self.platforms)],
                            'retention_days': settings.DEFAULT_RETENTION_DAYS,
                            'data': {
                                # Project N sends every Nth (mod len(hashes)) hash (and platform)
                                'received': calendar.timegm((self.base_time + timedelta(minutes=tick)).timetuple()),
                                'type': 'transaction',
                                'transaction': '/api/do_things',

                                'start_timestamp': datetime.timestamp(self.base_time + timedelta(minutes=tick)),
                                'timestamp': datetime.timestamp(self.base_time + timedelta(minutes=tick, seconds=1)),
                                'tags': {
                                    # Sentry
                                    'environment': self.environments[(tock * p) % len(self.environments)],
                                    'sentry:release': str(tick),
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
                                        'timestamp': calendar.timegm((self.base_time + timedelta(minutes=tick)).timetuple()),
                                    }
                                ]
                            }
                        }))
                    events.extend(processed.data)
        self.write_processed_events(events)

    def test_read_ip(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': ['transaction_name', 'ip_address_v4', 'ip_address_v6'],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
            'orderby': 'start_ts'
        }))
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) > 1, data
        assert 'ip_address_v4' in data['data'][0]
        assert 'ip_address_v6' in data['data'][0]

    def test_read_lowcard(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': ['transaction_op', 'platform'],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
            'orderby': 'start_ts'
        }))
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) > 1, data
        assert 'platform' in data['data'][0]
        assert data['data'][0]['transaction_op'] == "http"

    def test_start_ts_microsecond_truncation(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': ['transaction_name'],
            'conditions': [
                ['start_ts', '>', (self.base_time - timedelta(minutes=180, microseconds=9876)).isoformat()],
                ['start_ts', '<', (self.base_time + timedelta(minutes=self.minutes, microseconds=9876)).isoformat()],
            ],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
            'orderby': 'start_ts'
        }))
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) > 1, data
        assert 'transaction_name' in data['data'][0]

    def test_split_query(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': [
                'event_id',
                'project_id',
                'transaction_name',
                'transaction_hash',
                'tags_key'
            ],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
        }))
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) > 1, data

    def test_column_formatting(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': [
                'event_id',
                'ip_address',
                'project_id',
            ],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
        }))

        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert len(data['data']) == 180
        first_event_id = data['data'][0]['event_id']
        assert len(first_event_id) == 32
        assert data['data'][0]['ip_address'] == '8.8.8.8'

        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': [
                'event_id',
                'project_id',
            ],
            'conditions': [['event_id', '=', first_event_id]],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
        }))

        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) == 1
        assert data['data'][0]['event_id'] == first_event_id

    def test_apdex_function(self):
        skew = timedelta(minutes=180)
        response = self.app.post('/query', data=json.dumps({
            'dataset': 'transactions',
            'project': 1,
            'selected_columns': ['transaction_name'],
            'aggregations': [['apdex(duration, 10)', '', 'apdex_score']],
            'from_date': (self.base_time - skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + timedelta(minutes=self.minutes)).replace(tzinfo=pytz.utc).isoformat(),
            'orderby': 'transaction_name',
            'groupby': ['transaction_name']
        }))
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data['data']) == 1, data
        assert 'apdex_score' in data['data'][0]
        assert data['data'][0] == {
            'transaction_name': '/api/do_things',
            'apdex_score': 0.5
        }
