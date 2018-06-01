# -*- coding: utf-8 -*-
import calendar
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
from functools import partial
import mock
import simplejson as json
import six
import time
import uuid
import pytest

from snuba import state, settings

from base import BaseTest


class TestApi(BaseTest):
    def setup_method(self, test_method):
        super(TestApi, self).setup_method(test_method)
        from snuba.api import application
        assert application.testing == True
        self.app = application.test_client()
        self.app.post = partial(self.app.post, headers={'referer': 'test'})

        # values for test data
        self.project_ids = [1, 2, 3]  # 3 projects
        self.environments = [u'prød', 'test']  # 2 environments
        self.platforms = ['a', 'b', 'c', 'd', 'e', 'f']  # 6 platforms
        self.hashes = [x * 32 for x in '0123456789ab']  # 12 hashes
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - \
            timedelta(minutes=self.minutes)
        self.generate_fizzbuzz_events()

    def teardown_method(self, test_method):
        # Reset rate limits
        state.delete_config('global_concurrent_limit')
        state.delete_config('global_per_second_limit')
        state.delete_config('project_concurrent_limit')
        state.delete_config('project_per_second_limit')

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
                    events.append({
                        'project_id': p,
                        'event_id': uuid.uuid4().hex,
                        'deleted': 0,
                        # Project N sends every Nth (mod len(hashes)) hash (and platform)
                        'platform': self.platforms[(tock * p) % len(self.platforms)],
                        'primary_hash': self.hashes[(tock * p) % len(self.hashes)],
                        'message': 'a message',
                        'timestamp': calendar.timegm((self.base_time + timedelta(minutes=tick)).timetuple()),
                        'received': calendar.timegm((self.base_time + timedelta(minutes=tick)).timetuple()),
                        'sentry:dist': 'dist1',
                        'sentry:release': six.text_type(tick),
                        'environment': self.environments[(tock * p) % len(self.environments)],
                        'tags.key': ['foo', 'foo.bar'],
                        'tags.value': ['baz', 'qux'],
                        'retention_days': settings.DEFAULT_RETENTION_DAYS,
                    })
        self.write_processed_events(events)

    def test_count(self):
        """
        Test total counts are correct in the hourly time buckets for each project
        """
        res = self.clickhouse.execute("SELECT count() FROM %s" % self.table)
        assert res[0][0] == 330

        rollup_mins = 60
        for p in self.project_ids:
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': p,
                'granularity': rollup_mins * 60,
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['count()', '', 'aggregate']],
                'groupby': 'time',
            })).data)
            buckets = self.minutes / rollup_mins
            for b in range(buckets):
                bucket_time = parse_datetime(result['data'][b]['time']).replace(tzinfo=None)
                assert bucket_time == self.base_time + timedelta(minutes=b * rollup_mins)
                assert result['data'][b]['aggregate'] == float(rollup_mins) / p

    def test_rollups(self):
        for rollup_mins in (1, 2, 15, 30, 60):
            # Note for buckets bigger than 1 hour, the results may not line up
            # with self.base_time as base_time is not necessarily on a bucket boundary
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 1,
                'granularity': rollup_mins * 60,
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['count()', '', 'aggregate']],
                'groupby': 'time',
            })).data)
            buckets = self.minutes / rollup_mins
            for b in range(buckets):
                bucket_time = parse_datetime(result['data'][b]['time']).replace(tzinfo=None)
                assert bucket_time == self.base_time + timedelta(minutes=b * rollup_mins)
                assert result['data'][b]['aggregate'] == rollup_mins  # project 1 has 1 event per minute

    def test_issues(self):
        """
        Test that issues are grouped correctly when passing an 'issues' list
        to the query.
        """
        for p in self.project_ids:
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': p,
                'granularity': 3600,
                'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
                'groupby': 'issue',
            })).data)
            issues_found = set([d['issue'] for d in result['data']])
            issues_expected = set(range(0, len(self.hashes), p))
            assert issues_found - issues_expected == set()

    def test_no_issues(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'issues': [],
            'groupby': 'issue',
        })).data)
        assert result['error'] is None

    def test_offset_limit(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': self.project_ids,
            'groupby': ['project_id'],
            'aggregations': [['count()', '', 'count']],
            'orderby': '-count',
            'offset': 1,
            'limit': 1,
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['project_id'] == 2

        # offset without limit is invalid
        result = self.app.post('/query', data=json.dumps({
            'project': self.project_ids,
            'groupby': ['project_id'],
            'aggregations': [['count()', '', 'count']],
            'orderby': '-count',
            'offset': 1,
        }))
        assert result.status_code == 400

    def test_conditions(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'issue',
            'conditions': [['issue', 'IN', [0, 1, 2, 3, 4]]]
        })).data)
        assert set([d['issue'] for d in result['data']]) == set([0, 4])

    def test_aggregate(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'project_id',
            'aggregations': [['topK(4)', 'issue', 'aggregate']],
        })).data)
        assert sorted(result['data'][0]['aggregate']) == [0, 3, 6, 9]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'project_id',
            'aggregations': [['uniq', 'issue', 'aggregate']],
        })).data)
        assert result['data'][0]['aggregate'] == 4

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
            'groupby': ['project_id', 'time'],
            'aggregations': [['uniq', 'issue', 'aggregate']],
        })).data)
        assert len(result['data']) == 3  # time buckets
        assert all(d['aggregate'] == 4 for d in result['data'])

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': self.project_ids,
            'groupby': ['project_id'],
            'aggregations': [
                ['count', 'platform', 'platforms'],
                ['uniq', 'platform', 'uniq_platforms'],
                ['topK(1)', 'platform', 'top_platforms'],
            ],
        })).data)
        data = sorted(result['data'], key=lambda r: r['project_id'])

        for idx, pid in enumerate(self.project_ids):
            assert data[idx]['project_id'] == pid
            assert data[idx]['uniq_platforms'] == len(self.platforms) // pid
            assert data[idx]['platforms'] == self.minutes // pid
            assert len(data[idx]['top_platforms']) == 1
            assert data[idx]['top_platforms'][0] in self.platforms

    def test_having_conditions(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'groupby': 'primary_hash',
            'having': [['times_seen', '>', 1]],
            'aggregations': [['count()', '', 'times_seen']],
        })).data)
        assert len(result['data']) == 3

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'groupby': 'primary_hash',
            'having': [['times_seen', '>', 100]],
            'aggregations': [['count()', '', 'times_seen']],
        })).data)
        assert len(result['data']) == 0

        with pytest.raises(AssertionError):
            # HAVING fails with no GROUP BY
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 2,
                'groupby': [],
                'having': [['times_seen', '>', 1]],
                'aggregations': [['count()', '', 'times_seen']],
            })).data)

        # unknown field times_seen
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'having': [['times_seen', '>', 1]],
            'groupby': 'time'
        })).data)
        assert result['error']

    def test_tag_expansion(self):
        # A promoted tag
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [['tags[sentry:dist]', 'IN', ['dist1', 'dist2']]],
            'aggregations': [['count()', '', 'aggregate']],
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
            ],
            'aggregations': [['count()', '', 'aggregate']],
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

        # A combination of promoted and nested
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [
                ['tags[sentry:dist]', '=', 'dist1'],
                ['tags[foo.bar]', '=', 'qux'],
            ],
            'aggregations': [['count()', '', 'aggregate']],
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

    @mock.patch('snuba.util.raw_query')
    def test_column_expansion(self, raw_query):
        # If there is a condition on an already SELECTed column, then use the
        # column alias instead of the full column expression again.
        raw_query.return_value = {'data': [], 'meta': []}
        issues = [(i, [j]) for i, j in enumerate(self.hashes)]
        json.loads(self.app.post('/query', data=json.dumps({
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
        assert "issue = 0" in sql
        assert "issue = 1" in sql

    def test_promoted_expansion(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'groupby': ['tags_key'],
            'aggregations': [
                ['count()', '', 'count'],
                ['uniq', 'tags_value', 'uniq'],
                ['topK(3)', 'tags_value', 'top'],
            ],
            'conditions': [
                ['tags_value', 'IS NOT NULL', None],
            ],
        })).data)

        result_map = {d['tags_key']: d for d in result['data']}
        # Result contains both promoted and regular tags
        assert set(result_map.keys()) == set([
            'foo', 'foo.bar', 'sentry:release', 'environment', 'sentry:dist'
        ])

        # Reguar (nested) tag
        assert result_map['foo']['count'] == 180
        assert len(result_map['foo']['top']) == 1
        assert result_map['foo']['top'][0] == 'baz'

        # Promoted tags
        assert result_map['sentry:release']['count'] == 180
        assert len(result_map['sentry:release']['top']) == 3
        assert result_map['sentry:release']['uniq'] == 180

        assert result_map['environment']['count'] == 180
        assert len(result_map['environment']['top']) == 2
        assert all(r in self.environments for r in result_map['environment']['top'])

    def test_unicode_condition(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'groupby': ['environment'],
            'aggregations': [
                ['count()', '', 'count']
            ],
            'conditions': [
                ['environment', 'IN', [u'prød']],
            ],
        })).data)
        assert result['data'][0] == {'environment': u'prød', 'count': 90}

    def test_query_timing(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'issues': [(i, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'issue',
        })).data)

        assert 'timing' in result
        assert 'timestamp' in result['timing']

    def test_rate_limiting(self):
        state.set_config('global_concurrent_limit', 0)
        response = self.app.post('/query', data=json.dumps({
            'project': 1,
        }))
        assert response.status_code == 429

    def test_doesnt_select_deletions(self):
        query = {
            'project': 1,
            'groupby': 'project_id',
            'aggregations': [
                ['count()', '', 'count']
            ]
        }
        result1 = json.loads(self.app.post('/query', data=json.dumps(query)).data)

        self.write_processed_events([{
            'event_id': '9' * 32,
            'project_id': 1,
            'timestamp': calendar.timegm(self.base_time.timetuple()),
            'deleted': 1,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
        }])

        result2 = json.loads(self.app.post('/query', data=json.dumps(query)).data)
        assert result1['data'] == result2['data']

    def test_select_columns(self):
        query = {
            'project': 1,
            'selected_columns': ['platform', 'message'],
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)

        assert len(result['data']) == 180
        assert result['data'][0] == {'message': 'a message', 'platform': 'b'}

    def test_test_endpoints(self):
        project_id = 73
        event = {
            'event_id': '9' * 32,
            'primary_hash': '1' * 32,
            'project_id': project_id,
            'datetime': self.base_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'deleted': 1,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
            'platform': 'python',
            'message': 'message',
            'data': {
                'received': time.mktime(self.base_time.timetuple()),
            }
        }
        response = self.app.post('/tests/insert', data=json.dumps([event]))
        assert response.status_code == 200

        query = {
            'project': project_id,
            'groupby': 'project_id',
            'aggregations': [
                ['count()', '', 'count']
            ]
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)
        assert result['data'] == [{'count': 1, 'project_id': project_id}]

        assert self.app.post('/tests/drop').status_code == 200

        assert settings.CLICKHOUSE_TABLE not in self.clickhouse.execute("SHOW TABLES")

    def test_issues_with_tombstone(self):
        now = datetime.utcnow()

        project_id = 100
        hash = 'a' * 32
        base_event = {
            'project_id': project_id,
            'event_id': uuid.uuid4().hex,
            'deleted': 0,
            'primary_hash': hash,
            'retention_days': 90,
        }

        event1 = base_event.copy()
        event1['timestamp'] = calendar.timegm((now - timedelta(days=1)).timetuple())

        event2 = base_event.copy()
        event2['timestamp'] = calendar.timegm((now - timedelta(days=3)).timetuple())

        self.write_processed_events([event1, event2])

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, [hash])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 2, 'issue': 0}]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, [(hash, None)])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 2, 'issue': 0}]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, [(hash, (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"))])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 1, 'issue': 0}]
