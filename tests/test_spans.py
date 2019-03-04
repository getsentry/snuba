from datetime import datetime
import json

from snuba import settings
from snuba.clickhouse import ClickhousePool
from snuba.redis import redis_client

class TestSpans(object):
    def setup_method(self, test_method):
        from snuba.api import application
        assert application.testing is True
        application.config['PROPAGATE_EXCEPTIONS'] = False
        assert settings.TESTING, "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.database = 'default'

        self.app = application.test_client()
        self.dataset = settings.get_dataset('events')
        self.clickhouse = ClickhousePool()

        self.clickhouse.execute(self.dataset.SCHEMA.get_local_table_drop())
        self.clickhouse.execute(self.dataset.SCHEMA.get_local_table_definition())
        redis_client.flushdb()

    def teardown_method(self, test_method):
        self.clickhouse.execute(self.dataset.SCHEMA.get_local_table_drop())
        redis_client.flushdb()

    def test_insert(self):
        events = [{
            'project_id': 123,
            'span_id': 'abc123',
            'user_id': 'me',
            'timestamp': self.base_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'duration': 1000,
            'transaction_id': 'asdfasdfasdf',
            'operation': 'thing_happened'
        }]

        response = self.app.post('/tests/spans/insert', data=json.dumps(events))
        assert response.status_code == 200

    def test_eventstream(self):
        message = (2, 'insert', {
            'project_id': 1234,
            'span_id': 'abc123',
            'user_id': 'me',
            'timestamp': self.base_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'duration': 1000,
            'transaction_id': 'asdfasdfasdf',
            'operation': 'thing_happened'
        })

        response = self.app.post('/tests/spans/eventstream', data=json.dumps(message))
        assert response.status_code == 200

