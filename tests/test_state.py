from base import BaseTest
import time
from mock import patch
import uuid

from snuba import state


class TestState(BaseTest):

    def test_concurrent_limit(self):
        # No concurrent limit
        with state.rate_limit('foo', concurrent_limit=None) as (allowed, _, _):
            assert allowed

        # 0 concurrent limit
        with state.rate_limit('foo', concurrent_limit=0) as (allowed, _, _):
            assert not allowed

        # Concurrent limit 1 with consecutive  queries
        with state.rate_limit('foo', concurrent_limit=1) as (allowed, _, _):
            assert allowed
        with state.rate_limit('foo', concurrent_limit=1) as (allowed, _, _):
            assert allowed

        # Concurrent limit with concurrent queries
        with state.rate_limit('foo', concurrent_limit=1) as (allowed1, _, _):
            with state.rate_limit('foo', concurrent_limit=1) as (allowed2, _, _):
                assert allowed1
                assert not allowed2

        # Concurrent with different buckets
        with state.rate_limit('foo', concurrent_limit=1) as (foo_allowed, _, _):
            with state.rate_limit('bar', concurrent_limit=1) as (bar_allowed, _, _):
                assert foo_allowed
                assert bar_allowed

    def test_per_second_limit(self):
        bucket = uuid.uuid4()
        # Create 30 queries at time 0, should all be allowed
        with patch.object(state.time, 'time', lambda: 0):
            for _ in range(30):
                with state.rate_limit(bucket, per_second_limit=1) as (allowed, _, _):
                    assert allowed

        # Create another 30 queries at time 30, should also be allowed
        with patch.object(state.time, 'time', lambda: 30):
            for _ in range(30):
                with state.rate_limit(bucket, per_second_limit=1) as (allowed, _, _):
                    assert allowed

        with patch.object(state.time, 'time', lambda: 60):
            # 1 more query should be allowed at T60 because it does not make the previous
            # rate exceed 1/sec until it has finished.
            with state.rate_limit(bucket, per_second_limit=1) as (allowed, _, _):
                assert allowed

            # But the next one should not be allowed
            with state.rate_limit(bucket, per_second_limit=1) as (allowed, _, _):
                assert not allowed

        # Another query at time 61 should be allowed because the first 30 queries
        # have fallen out of the lookback window
        with patch.object(state.time, 'time', lambda: 61):
            with state.rate_limit(bucket, per_second_limit=1) as (allowed, _, _):
                assert allowed

    def test_config(self):
        state.set_config('foo', 1)
        state.set_configs({'bar': 2, 'baz': 3})
        assert state.get_config('foo') == 1
        assert state.get_config('bar') == 2
        assert state.get_configs() == {'foo': 1, 'bar': 2, 'baz': 3}

        state.set_configs({'bar': 'quux'})
        assert state.get_configs() == {'foo': 1, 'bar': 'quux', 'baz': 3}
