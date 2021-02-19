import pytest
from unittest.mock import patch
import uuid

from snuba import state
from snuba.state.rate_limit import (
    rate_limit,
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitParameters,
    RateLimitStats,
    RateLimitStatsContainer,
)


class TestRateLimit:
    def test_concurrent_limit(self) -> None:
        # No concurrent limit should not raise
        rate_limit_params = RateLimitParameters("foo", "bar", None, None)
        with rate_limit(rate_limit_params) as stats:
            assert stats is not None

        # 0 concurrent limit
        rate_limit_params = RateLimitParameters("foo", "bar", None, 0)

        with pytest.raises(RateLimitExceeded):
            with rate_limit(rate_limit_params):
                pass

        # Concurrent limit 1 with consecutive queries should not raise
        rate_limit_params = RateLimitParameters("foo", "bar", None, 1)

        with rate_limit(rate_limit_params):
            pass

        with rate_limit(rate_limit_params):
            pass

        # Concurrent limit with concurrent queries
        rate_limit_params = RateLimitParameters("foo", "bar", None, 1)

        with pytest.raises(RateLimitExceeded):
            with rate_limit(rate_limit_params):
                with rate_limit(rate_limit_params):
                    pass

        # Concurrent with different buckets should not raise
        rate_limit_params1 = RateLimitParameters("foo", "bar", None, 1)
        rate_limit_params2 = RateLimitParameters("shoe", "star", None, 1)

        with RateLimitAggregator([rate_limit_params1]):
            with RateLimitAggregator([rate_limit_params2]):
                pass

    def test_per_second_limit(self) -> None:
        bucket = uuid.uuid4()
        rate_limit_params = RateLimitParameters("foo", bucket, 1, None)
        # Create 30 queries at time 0, should all be allowed
        with patch.object(state.time, "time", lambda: 0):
            for _ in range(30):
                with rate_limit(rate_limit_params) as stats:
                    assert stats is not None

        # Create another 30 queries at time 30, should also be allowed
        with patch.object(state.time, "time", lambda: 30):
            for _ in range(30):
                with rate_limit(rate_limit_params) as stats:
                    assert stats is not None

        with patch.object(state.time, "time", lambda: 60):
            # 1 more query should be allowed at T60 because it does not make the previous
            # rate exceed 1/sec until it has finished.
            with rate_limit(rate_limit_params) as stats:
                assert stats is not None

            # But the next one should not be allowed
            with pytest.raises(RateLimitExceeded):
                with rate_limit(rate_limit_params):
                    pass

        # Another query at time 61 should be allowed because the first 30 queries
        # have fallen out of the lookback window
        with patch.object(state.time, "time", lambda: 61):
            with rate_limit(rate_limit_params) as stats:
                assert stats is not None

    def test_aggregator(self) -> None:
        # do not raise with multiple valid rate limits
        rate_limit_params_outer = RateLimitParameters("foo", "bar", None, 5)
        rate_limit_params_inner = RateLimitParameters("foo", "bar", None, 5)

        with RateLimitAggregator([rate_limit_params_outer, rate_limit_params_inner]):
            pass

        # raise when the inner rate limit should fail
        rate_limit_params_outer = RateLimitParameters("foo", "bar", None, 0)
        rate_limit_params_inner = RateLimitParameters("foo", "bar", None, 5)

        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator(
                [rate_limit_params_outer, rate_limit_params_inner]
            ):
                pass

        # raise when the outer rate limit should fail
        rate_limit_params_outer = RateLimitParameters("foo", "bar", None, 5)
        rate_limit_params_inner = RateLimitParameters("foo", "bar", None, 0)

        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator(
                [rate_limit_params_outer, rate_limit_params_inner]
            ):
                pass

    def test_rate_limit_container(self) -> None:
        rate_limit_container = RateLimitStatsContainer()
        rate_limit_stats = RateLimitStats(rate=0.5, concurrent=2)

        rate_limit_container.add_stats("foo", rate_limit_stats)

        assert rate_limit_container.get_stats("foo") == rate_limit_stats
        assert rate_limit_container.get_stats("bar") is None

        assert rate_limit_container.to_dict() == {"foo_rate": 0.5, "foo_concurrent": 2}

    def test_bypass_rate_limit(self) -> None:
        rate_limit_params = RateLimitParameters("foo", "bar", None, None)
        state.set_config("bypass_rate_limit", 1)

        with rate_limit(rate_limit_params) as stats:
            assert stats is None
