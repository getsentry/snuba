# -*- coding: utf-8 -*-
import calendar
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
from dateutil.tz import tz
from functools import partial
import simplejson as json
import six
import time
import uuid
import pytest
import pytz

from snuba import processor, settings, state

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
                    events.append(processor.process_insert({
                        'project_id': p,
                        'event_id': uuid.uuid4().hex,
                        'deleted': 0,
                        'datetime': (self.base_time + timedelta(minutes=tick)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        'message': 'a message',
                        'platform': self.platforms[(tock * p) % len(self.platforms)],
                        'primary_hash': self.hashes[(tock * p) % len(self.hashes)],
                        'retention_days': settings.DEFAULT_RETENTION_DAYS,
                        'data': {
                            # Project N sends every Nth (mod len(hashes)) hash (and platform)
                            'received': calendar.timegm((self.base_time + timedelta(minutes=tick)).timetuple()),
                            'tags': {
                                # Sentry
                                'environment': self.environments[(tock * p) % len(self.environments)],
                                'sentry:release': six.text_type(tick),
                                'sentry:dist': 'dist1',
                                'os.name': 'windows',
                                'os.rooted': 1,

                                # User
                                'foo': 'baz',
                                'foo.bar': 'qux',
                                'os_name': 'linux',
                            },
                        }
                    }))
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
                'orderby': 'time',
                'groupby': 'time',
            })).data)
            buckets = self.minutes / rollup_mins
            for b in range(int(buckets)):
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
                'orderby': 'time',
            })).data)
            buckets = self.minutes / rollup_mins
            for b in range(int(buckets)):
                bucket_time = parse_datetime(result['data'][b]['time']).replace(tzinfo=None)
                assert bucket_time == self.base_time + timedelta(minutes=b * rollup_mins)
                assert result['data'][b]['aggregate'] == rollup_mins  # project 1 has 1 event per minute

    def test_time_alignment(self):
        # Adding a half hour skew to the time.
        skew = timedelta(minutes=30)
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 60,
            'groupby': 'time',
            'from_date': (self.base_time + skew).replace(tzinfo=pytz.utc).isoformat(),
            'to_date': (self.base_time + skew + timedelta(minutes=self.minutes)).isoformat(),
            'orderby': 'time'
        })).data)
        bucket_time = parse_datetime(result['data'][0]['time']).replace(tzinfo=None)
        assert bucket_time == (self.base_time + skew)

        # But if we set time alignment to an hour, the buckets will fall back to
        # the 1hr boundary.
        state.set_config('date_align_seconds', 3600)
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 60,
            'groupby': 'time',
            'from_date': (self.base_time + skew).isoformat(),
            'to_date': (self.base_time + skew + timedelta(minutes=self.minutes)).isoformat(),
            'orderby': 'time'
        })).data)
        bucket_time = parse_datetime(result['data'][0]['time']).replace(tzinfo=None)
        assert bucket_time == self.base_time

    def test_issues(self):
        """
        Test that issues are grouped correctly when passing an 'issues' list
        to the query.
        """
        for p in self.project_ids:
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': p,
                'granularity': 3600,
                'issues': [(i, p, [j]) for i, j in enumerate(self.hashes)],
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
        assert 'error' not in result

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'groupby': 'issue',
            'conditions': [['issue', '=', 100]],
        })).data)
        assert 'error' not in result
        assert result['data'] == []

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'groupby': 'issue',
            'conditions': [['issue', 'IN', [100, 200]]],
        })).data)
        assert 'error' not in result
        assert result['data'] == []

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

        # limit over max-limit is invalid
        result = self.app.post('/query', data=json.dumps({
            'project': self.project_ids,
            'groupby': ['project_id'],
            'aggregations': [['count()', '', 'count']],
            'orderby': '-count',
            'limit': 1000000,
        }))
        assert result.status_code == 400

    def test_conditions(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'issues': [(i, 2, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'issue',
            'conditions': [[], ['issue', 'IN', [0, 1, 2, 3, 4]]]
        })).data)
        assert set([d['issue'] for d in result['data']]) == set([0, 4])

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'aggregations': [['count()', '', 'count']],
            'groupby': 'platform',
            'conditions': [['platform', 'NOT IN', ['b', 'c', 'd', 'e', 'f']]]
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['platform'] == 'a'

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'selected_columns': ['event_id'],
            'conditions': [['message', 'LIKE', 'a mess%']],
            'orderby': 'event_id',
            'limit': 1,
        })).data)
        assert len(result['data']) == 1

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'selected_columns': ['event_id'],
            'conditions': [['message', 'NOT LIKE', 'a mess%']],
            'orderby': 'event_id',
            'limit': 1,
        })).data)
        assert len(result['data']) == 0

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'selected_columns': ['event_id'],
            'conditions': [['tags[environment]', 'LIKE', '%es%']],
            'orderby': 'event_id',
            'limit': 1,
        })).data)
        assert len(result['data']) == 1

    def test_aggregate(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, 3, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'project_id',
            'aggregations': [['topK(4)', 'issue', 'aggregate']],
        })).data)
        assert sorted(result['data'][0]['aggregate']) == [0, 3, 6, 9]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, 3, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'project_id',
            'aggregations': [['uniq', 'issue', 'aggregate']],
        })).data)
        assert result['data'][0]['aggregate'] == 4

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': [(i, 3, [j]) for i, j in enumerate(self.hashes)],
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

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'granularity': 3600,
            'groupby': 'project_id',
            'conditions': [
                ['tags[os.rooted]', '=', '1'],
            ],
            'aggregations': [['count()', '', 'aggregate']],
        })).data)
        assert len(result['data']) == 1
        assert result['data'][0]['aggregate'] == 90

    def test_column_expansion(self):
        # If there is a condition on an already SELECTed column, then use the
        # column alias instead of the full column expression again.
        issues = [(i, 2, [j]) for i, j in enumerate(self.hashes)]
        response = json.loads(self.app.post('/query', data=json.dumps({
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
        assert "issue = 0" in response['sql']
        assert "issue = 1" in response['sql']

    def test_sampling_expansion(self):
        response = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'sample': 1000,
        })).data)
        assert "SAMPLE 1000" in response['sql']

        response = json.loads(self.app.post('/query', data=json.dumps({
            'project': 2,
            'sample': 0.1,
        })).data)
        assert "SAMPLE 0.1" in response['sql']

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
            # Promoted tags
            'environment',
            'sentry:dist',
            'sentry:release',
            'os.rooted',
            'os.name',

            # User (nested) tags
            'foo',
            'foo.bar',
            # Note this is a nested (user-provided) os_name tag and is
            # unrelated to the fact that we happen to store the
            # `os.name` tag as an `os_name` column.
            'os_name',
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

    def test_tag_translation(self):
        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 1,
            'granularity': 3600,
            'aggregations': [
                ['topK(100)', 'tags_key', 'top'],
            ],
        })).data)

        assert 'os.rooted' in result['data'][0]['top']

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
            'issues': [(i, 1, [j]) for i, j in enumerate(self.hashes)],
            'groupby': 'issue',
        })).data)

        assert 'timing' in result
        assert 'timestamp' in result['timing']

    def test_global_rate_limiting(self):
        state.set_config('global_concurrent_limit', 0)
        response = self.app.post('/query', data=json.dumps({
            'project': 1,
        }))
        assert response.status_code == 429

    def test_project_rate_limiting(self):
        # All projects except project 1 are allowed
        state.set_config('project_concurrent_limit', 1)
        state.set_config('project_concurrent_limit_1', 0)

        response = self.app.post('/query', data=json.dumps({
            'project': 2,
            'selected_columns': ['platform']
        }))
        assert response.status_code == 200

        response = self.app.post('/query', data=json.dumps({
            'project': 1,
            'selected_columns': ['platform']
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
            'timestamp': self.base_time,
            'deleted': 1,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
        }])

        result2 = json.loads(self.app.post('/query', data=json.dumps(query)).data)
        assert result1['data'] == result2['data']

    def test_selected_columns(self):
        query = {
            'project': 1,
            'selected_columns': ['platform', 'message'],
            'orderby': 'platform',
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)

        assert len(result['data']) == 180
        assert result['data'][0] == {'message': 'a message', 'platform': 'a'}

    def test_complex_selected_columns(self):
        query = {
            'project': 1,
            'selected_columns': ['platform', ['notEmpty', ['exception_stacks.type']]],
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)
        assert len(result['data']) == 180
        assert 'platform' in result['data'][0]
        assert 'notEmpty(exception_stacks.type)' in result['data'][0]
        assert result['data'][0]['notEmpty(exception_stacks.type)'] == 0

        # Check that aliasing works too
        query = {
            'project': 1,
            'selected_columns': ['platform', ['notEmpty', ['exception_stacks.type'], 'type_not_empty']],
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)
        assert len(result['data']) == 180
        assert 'platform' in result['data'][0]
        assert 'type_not_empty' in result['data'][0]
        assert result['data'][0]['type_not_empty'] == 0

    def test_nullable_datetime_columns(self):
        # Test that requesting a Nullable(DateTime) column does not throw
        query = {
            'project': 1,
            'selected_columns': ['received'],
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)

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
        event1['timestamp'] = now - timedelta(days=1)

        event2 = base_event.copy()
        event2['timestamp'] = now - timedelta(days=3)

        self.write_processed_events([event1, event2])

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, project_id, [hash])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 2, 'issue': 0}]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, project_id, [(hash, None)])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 2, 'issue': 0}]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': project_id,
            'issues': [(0, project_id, [(hash, (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"))])],
            'groupby': 'issue',
            'aggregations': [
                ['count()', '', 'count']
            ]
        })).data)
        assert result['data'] == [{'count': 1, 'issue': 0}]

    @pytest.mark.xfail
    def test_row_stats(self):
        query = {
            'project': 1,
            'selected_columns': ['platform'],
        }
        result = json.loads(self.app.post('/query', data=json.dumps(query)).data)

        assert 'rows_read' in result['stats']
        assert 'bytes_read' in result['stats']
        assert result['stats']['bytes_read'] > 0

    def test_multi_project_issues(self):
        hash = 'a' * 32

        self.write_processed_events(processor.process_insert({
            'project_id': 2,
            'event_id': uuid.uuid4().hex,
            'deleted': 0,
            'datetime': (self.base_time).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'message': 'a message',
            'platform': self.platforms[0],
            'primary_hash': hash,
            'retention_days': settings.DEFAULT_RETENTION_DAYS,
            'data': {
                'received': calendar.timegm((self.base_time).timetuple()),
            }
        }))

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': [1, 2],
            'issues': [
                (1, 1, [(hash, None)]),
                (2, 2, [(hash, None)]),
            ],
            'groupby': ['project_id', 'issue'],
            'aggregations': [
                ['count()', '', 'count']
            ],
            'orderby': '-count',
        })).data)

        assert result['data'] == [
            # 15 events naturally exist (via the setup method) for project 1
            # with hash 'a' * 32, under issue 1
            {'count': 15, 'issue': 1, 'project_id': 1},
            # 1 event was created under project 2 with hash 'a' * 32, under
            # issue 2
            {'count': 1, 'issue': 2, 'project_id': 2},
        ]

    def test_generalizer(self):
        try:
            state.set_config('use_cache', 1)
            state.set_config('use_query_id', 1)
            state.set_config('generalize_query', 1)

            # First get the results for 1 tag
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 1,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['count()', '', 'count']],
                'conditions': [
                    ['tags[os.name]', '!=', ''],
                    ['environment', '=', 'test']
                ],
                'orderby': '-count',
            })).data)
            assert result['data'] == [{'count': 90}]
            assert result['stats']['cache_hit'] == False
            query_1_id = result['stats']['query_id']

            # Then get the results for another tag, which we should be able
            # to serve from the cached resut from the first query
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 1,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['count()', '', 'count']],
                'conditions': [
                    ['tags[sentry:dist]', '!=', ''],
                    ['environment', '=', 'test']
                ],
                'orderby': '-count',
            })).data)
            assert result['data'] == [{'count': 90}]
            assert result['stats']['cache_hit'] == True
            result['stats']['query_id'] == query_1_id

            # Example 2: uniqes
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 1,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['uniq', 'tags[sentry:release]', 'unique_values']],
                'conditions': [
                    ['tags[sentry:release]', '!=', ''],

                    ['environment', '=', 'test']
                ],
                'orderby': '-unique_values',
            })).data)
            assert result['data'] == [{'unique_values': 90}]
            assert result['stats']['cache_hit'] == False
            query_1_id = result['stats']['query_id']

            # Then get the results for another tag, which we should be able
            # to serve from the cached resut from the first query
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 1,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [['uniq', 'tags[os.name]', 'unique_values']],
                'conditions': [
                    ['tags[os.name]', '!=', ''],
                    ['environment', '=', 'test']
                ],
                'orderby': '-unique_values',
            })).data)
            assert result['data'] == [{'unique_values': 1}]
            assert result['meta'] == [{
                'name': 'unique_values',
                'type': 'UInt64'
            }]
            assert result['stats']['cache_hit'] == True
            result['stats']['query_id'] == query_1_id

            # Example 3: top values for key
            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 3,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [
                    ['count()', '', 'times_seen'],
                    ['min', 'timestamp', 'first_seen']
                ],
                'conditions': [
                    ['tags[sentry:release]', '!=', ''],
                    ['environment', '=', 'test']
                ],
                'groupby': ['tags[sentry:release]'],
                'orderby': '-times_seen',
                'limit': 30,
            })).data)
            assert len(result['data']) == 30
            assert sorted([int(d['tags[sentry:release]']) for d in result['data']]) == range(2, self.minutes, 6)
            assert result['stats']['cache_hit'] == False
            query_1_id = result['stats']['query_id']

            result = json.loads(self.app.post('/query', data=json.dumps({
                'project': 3,
                'groupby': [],
                'from_date': self.base_time.isoformat(),
                'to_date': (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
                'aggregations': [
                    ['count()', '', 'times_seen'],
                    ['min', 'timestamp', 'first_seen']
                ],
                'conditions': [
                    ['tags[os.name]', '!=', ''],
                    ['environment', '=', 'test']
                ],
                'groupby': ['tags[os.name]'],
                'orderby': '-times_seen',
                'limit': 30,
            })).data)
            assert result['data'] == [{
                'times_seen': 30,
                'tags[os.name]': 'windows',
                'first_seen': (self.base_time + timedelta(minutes=2)).replace(tzinfo=tz.tzutc()).isoformat(),
            }]
            assert result['stats']['cache_hit'] == True

        finally:
            state.delete_config('use_query_id')
            state.delete_config('use_cache')
            state.delete_config('generalize_query')
