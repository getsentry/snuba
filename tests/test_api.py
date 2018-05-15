# -*- coding: utf-8 -*-
import calendar
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_datetime
import mock
import random
import simplejson as json
import six
import sys
import time
import uuid
import pytest

from snuba import util, state, settings

from base import BaseTest


class TestApi(BaseTest):

    def setup_method(self, test_method):
        super(TestApi, self).setup_method(test_method)
        from snuba.api import application
        assert application.testing == True
        self.app = application.test_client()

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
                # project N sends an event every Nth second
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
                        'dist': 'dist1',
                        'release': six.text_type(tick),
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
            'aggregations': [['topK(4)', 'issue', 'aggregate']],
        })).data)
        assert sorted(result['data'][0]['aggregate']) == [0, 3, 6, 9]

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': list(enumerate(self.hashes)),
            'groupby': 'project_id',
            'aggregations': [['uniq', 'issue', 'aggregate']],
        })).data)
        assert result['data'][0]['aggregate'] == 4

        result = json.loads(self.app.post('/query', data=json.dumps({
            'project': 3,
            'issues': list(enumerate(self.hashes)),
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

    def test_having_condiitons(self):
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
        })).data)
        assert result['error']

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
            'foo', 'foo.bar', 'release', 'environment', 'dist'
        ])

        # Reguar (nested) tag
        assert result_map['foo']['count'] == 180
        assert len(result_map['foo']['top']) == 1
        assert result_map['foo']['top'][0] == 'baz'

        # Promoted tags
        assert result_map['release']['count'] == 180
        assert len(result_map['release']['top']) == 3
        assert result_map['release']['uniq'] == 180

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
            'issues': list(enumerate(self.hashes)),
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
