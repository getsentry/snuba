import time
import uuid
from typing import Tuple
from unittest.mock import patch

import pytest

from snuba import state
from snuba.redis import RedisClientKey, RetryingStrictRedisCluster, get_redis_client
from snuba.state.rate_limit import (
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitParameters,
    RateLimitStats,
    RateLimitStatsContainer,
    rate_limit,
    redis_clients,
)


@pytest.fixture(params=[1, 20])
def rate_limit_shards(request):
    """
    Use this fixture to run the test automatically against both 1
    and 20 shards.
    """
    state.set_config("rate_limit_shard_factor", request.param)


class TestRateLimit:
    def test_concurrent_limit(self, rate_limit_shards) -> None:
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

    def test_per_second_limit(self, rate_limit_shards) -> None:
        bucket = uuid.uuid4()
        rate_limit_params = RateLimitParameters("foo", str(bucket), 1, None)
        # Create 30 queries at time 0, should all be allowed
        with patch.object(state.time, "time", lambda: 0):  # type: ignore
            for _ in range(30):
                with rate_limit(rate_limit_params) as stats:
                    assert stats is not None

        # Create another 30 queries at time 30, should also be allowed
        with patch.object(state.time, "time", lambda: 30):  # type: ignore
            for _ in range(30):
                with rate_limit(rate_limit_params) as stats:
                    assert stats is not None

        with patch.object(state.time, "time", lambda: 60):  # type: ignore
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
        with patch.object(state.time, "time", lambda: 61):  # type: ignore
            with rate_limit(rate_limit_params) as stats:
                assert stats is not None

    def test_aggregator(self, rate_limit_shards) -> None:
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

        assert dict(rate_limit_container.to_dict()) == {
            "foo_rate": 0.5,
            "foo_concurrent": 2,
        }

    def test_bypass_rate_limit(self) -> None:
        rate_limit_params = RateLimitParameters("foo", "bar", None, None)
        state.set_config("bypass_rate_limit", 1)

        with rate_limit(rate_limit_params) as stats:
            assert stats is None

    def test_rate_limit_exceptions(self) -> None:
        params = RateLimitParameters("foo", "bar", None, 5)
        bucket = "{}{}".format(state.ratelimit_prefix, params.bucket)

        def count() -> int:
            return get_redis_client(RedisClientKey.RATE_LIMITER).zcount(
                bucket, "-inf", "+inf"
            )

        with rate_limit(params):
            assert count() == 1

        assert count() == 1

        with pytest.raises(RateLimitExceeded):
            with rate_limit(params):
                assert count() == 2
                raise RateLimitExceeded(
                    "stuff"
                )  # simulate an inner rate limiter failing

        assert count() == 2


tests = [
    pytest.param((0, 5, 5)),
    pytest.param((5, 0, 5)),
    pytest.param((5, 5, 0)),
]


@pytest.mark.parametrize(
    "vals",
    tests,
)
def test_rate_limit_failures(vals: Tuple[int, int, int], rate_limit_shards) -> None:
    params = []
    for i, v in enumerate(vals):
        params.append(RateLimitParameters(f"foo{i}", f"bar{i}", None, v))

    with pytest.raises(RateLimitExceeded):
        with RateLimitAggregator(params):
            pass

    now = time.time()
    for p in params:
        bucket = "{}{}".format(state.ratelimit_prefix, p.bucket)
        count = get_redis_client(RedisClientKey.RATE_LIMITER).zcount(
            bucket, now - state.rate_lookback_s, now + state.rate_lookback_s
        )
        assert count == 0


@pytest.mark.skipif(
    isinstance(redis_clients[0], RetryingStrictRedisCluster),
    reason="test requires support for db parameter, and redis cluster does not support DBs",
)
def test_rate_limit_v2_cluster(snuba_set_config):
    params = RateLimitParameters("foo", "foo", None, 4)

    # Start 4 concurrent "queries"... concurrent limit maxed out
    with RateLimitAggregator([params] * 4):
        # Exceed concurrent rate limit
        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator([params] * 4):
                pass

        # switch over to new cluster
        snuba_set_config("rate_limit_use_v2_cluster", 1.0)

        # since we switched over to a new cluster, the limit is
        # practically reset
        with RateLimitAggregator([params] * 4):
            pass

        snuba_set_config("rate_limit_use_v2_cluster", 0.0)

        # assert that rollback succeeds and we exceed the limit
        # again (since we're talking to the old cluster again)
        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator([params] * 4):
                pass


@pytest.mark.skipif(
    isinstance(redis_clients[0], RetryingStrictRedisCluster),
    reason="test requires support for db parameter, and redis cluster does not support DBs",
)
def test_rate_limit_v2_cluster_inconsistency(snuba_set_config):
    params = RateLimitParameters("foo", "foo", None, 4)

    # max out rate limit
    with RateLimitAggregator([params] * 4):
        snuba_set_config("rate_limit_use_v2_cluster", 1.0)

    # asserted that rate limiter does not crash if cluster is switched
    # halfway through (since the routing decision is only made once)

    # check that rollback does not crash either
    with RateLimitAggregator([params] * 4):
        snuba_set_config("rate_limit_use_v2_cluster", 0.0)

    with pytest.raises(RateLimitExceeded):
        with RateLimitAggregator([params] * 5):
            snuba_set_config("rate_limit_use_v2_cluster", 1.0)

    snuba_set_config("rate_limit_use_v2_cluster", 0.0)

    # assert that rate limits don't get "stuck" when clusters are switched
    # while a query is rejected, and we can still run queries after
    with RateLimitAggregator([params] * 4):
        pass
