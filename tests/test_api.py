import calendar
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
import json
import mock
import random
import sys
import time
import uuid

from snuba import util

from base import BaseTest


class TestApi(BaseTest):

    def setup_method(self, test_method):
        super(TestApi, self).setup_method(test_method)
        from snuba import settings
        settings.CLICKHOUSE_TABLE = 'test'
        settings.CLICKHOUSE_PORT = 9000
        from snuba import api
        api.app.testing = True
        api.clickhouse = self.conn
        self.app = api.app.test_client()

        # values for test data
        self.project_ids = [1, 2, 3]  # 3 projects
        self.platforms = ['a', 'b', 'c', 'd', 'e', 'f']  # 6 platforms
        self.hashes = [x * 32 for x in '0123456789ab']  # 12 hashes
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - \
            timedelta(minutes=self.minutes)
        self.generate_fizzbuzz_events()

    def generate_fizzbuzz_events(self):
        """
        Generate a deterministic set of events across a time range.
        """

        events = []
        for tick in range(self.minutes):
            tock = tick + 1
            for p in self.project_ids:
                # project N sends an event every Nth second
                if tock % p == 0:
                    events.append({
                        'project_id': p,
                        'event_id': uuid.uuid4().hex,
                        # Project N sends every Nth (mod len(hashes)) hash (and platform)
                        'platform': self.platforms[(tock * p) % len(self.platforms)],
                        'primary_hash': self.hashes[(tock * p) % len(self.hashes)],
                        'message': 'a message',
                        'timestamp': time.mktime((self.base_time + timedelta(minutes=tick)).timetuple()),
                        'received': time.mktime((self.base_time + timedelta(minutes=tick)).timetuple()),
                        'dist': 'dist1',
                        'tags.key': ['foo', 'foo.bar'],
                        'tags.value': ['baz', 'qux'],
                    })
        self.write_processed_events(events)

    def test_count(self):
        """
        Test total counts are correct in the hourly time buckets for each project
        """
        res = self.conn.execute("SELECT count() FROM %s" % self.table)
        assert res[0][0] == 330

        rollup_mins = 60
        for p in self.project_ids:
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': p,
                'granularity': rollup_mins * 60,
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat()
            })).data)
            buckets = self.minutes / rollup_mins
            for b in range(buckets):
                bucket_time = parse_datetime(result['data'][b]['time']).replace(tzinfo=None)
                assert bucket_time == self.base_time + timedelta(minutes=b * rollup_mins)
                assert result['data'][b]['aggregate'] == float(rollup_mins) / p

    def test_issues(self):
        """
        Test that issues are grouped correctly when passing an 'issues' list
        to the query.
        """
        for p in self.project_ids:
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': p,
                'granularity': 3600,
                'issues': list(enumerate(self.hashes)),
                'groupby': 'issue',
            })).data)
            issues_found = set([d['issue'] for d in result['data']])
            issues_expected = set(range(0, len(self.hashes), p))
            assert issues_found - issues_expected == set()

    def test_conditions(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'issues': list(enumerate(self.hashes)),
            'groupby': 'issue',
            'conditions': [['issue', 'IN', [0, 1, 2, 3, 4]]]
        })).data)
        assert set([d['issue'] for d in result['data']]) == set([0, 4])

    def test_aggregate(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': list(enumerate(self.hashes)),
            'groupby': 'project_id',
            'aggregation': 'topK(4)',
            'aggregateby': 'issue',
        })).data)
        assert sorted(result['data'][0]['aggregate']) == [0, 3, 6, 9]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': list(enumerate(self.hashes)),
            'groupby': 'project_id',
            'aggregation': 'uniq',
            'aggregateby': 'issue',
        })).data)
        assert result['data'][0]['aggregate'] == 4

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': list(enumerate(self.hashes)),
            'groupby': ['project_id', 'time'],
            'aggregation': 'uniq',
            'aggregateby': 'issue',
        })).data)
        assert len(result['data']) == 3  # time buckets
        assert all(d['aggregate'] == 4 for d in result['data'])

    def test_tag_expansion(self):
        # A promoted tag
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [['tags[dist]', 'IN', ['dist1', 'dist2']]]
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

        # A non promoted tag
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [
                ['tags[foo]', '=', 'baz'],
                ['tags[foo.bar]', '=', 'qux'],
            ]
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

        # A combination of promoted and nested
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [
                ['tags[dist]', '=', 'dist1'],
                ['tags[foo.bar]', '=', 'qux'],
            ]
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

    @mock.patch('snuba.util.raw_query')
    def test_column_expansion(self, raw_query):
        # If there is a condition on an already SELECTed column, then use the
        # column alias instead of the full column expression again.
        raw_query.return_value = {'data': [], 'meta': []}
        issues = list(enumerate(self.hashes))
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'issue',
            'issues': issues,
            'conditions': [
                ['issue', '=', 0],
                ['issue', '=', 1],
            ]
        })).data)
        # Issue is expanded once, and alias used subsequently
        sql = raw_query.call_args[0][0]
        assert util.issue_expr(issues[:2]) + ' AS issue' in sql
        assert "issue = 0" in sql
        assert "issue = 1" in sql
