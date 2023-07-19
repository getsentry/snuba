from __future__ import annotations

import time
import uuid
from typing import Any, Tuple
from unittest.mock import patch

import pytest

from snuba import state
from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.rate_limit import (
    RateLimitAggregator,
    RateLimitExceeded,
    RateLimitParameters,
    RateLimitStats,
    RateLimitStatsContainer,
    get_rate_limit_config,
    rate_limit,
    set_rate_limit_config,
)


@pytest.fixture(params=[1, 20])
def rate_limit_shards(request: Any) -> None:
    """
    Use this fixture to run the test automatically against both 1
    and 20 shards.
    """
    state.set_config("rate_limit_shard_factor", request.param)


class TestRateLimit:
    @pytest.mark.redis_db
    def test_concurrent_limit(self, rate_limit_shards: Any) -> None:
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

    @pytest.mark.redis_db
    def test_fails_open(self, rate_limit_shards: Any) -> None:
        with patch("snuba.state.rate_limit.rds.pipeline") as pipeline:
            pipeline.execute.side_effect = Exception("Boom!")
            rate_limit_params = RateLimitParameters("foo", "bar", 4, 20)
            with rate_limit(rate_limit_params):
                pass

    @pytest.mark.redis_db
    def test_per_second_limit(self, rate_limit_shards: Any) -> None:
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

    @pytest.mark.redis_db
    def test_aggregator(self, rate_limit_shards: Any) -> None:
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

    @pytest.mark.redis_db
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

    @pytest.mark.redis_db
    def test_bypass_rate_limit(self) -> None:
        rate_limit_params = RateLimitParameters("foo", "bar", None, None)
        state.set_config("bypass_rate_limit", 1)

        with rate_limit(rate_limit_params) as stats:
            assert stats is None

    @pytest.mark.redis_db
    def test_rate_limit_exceptions(self) -> None:
        params = RateLimitParameters("foo", "bar", None, 5)
        bucket = "{}{}".format(state.ratelimit_prefix, params.bucket)

        def count() -> int:
            return int(
                get_redis_client(RedisClientKey.RATE_LIMITER).zcount(
                    bucket, "-inf", "+inf"
                )
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

    @pytest.mark.redis_db
    def test_rate_limit_ttl(self) -> None:
        params = RateLimitParameters("foo", "bar", None, 5)
        bucket = "{}{}".format(state.ratelimit_prefix, params.bucket)

        with rate_limit(params):
            pass

        ttl = get_redis_client(RedisClientKey.RATE_LIMITER).ttl(bucket)
        assert 0 < ttl <= 3600 + 60 + 1


tests = [
    pytest.param((0, 5, 5)),
    pytest.param((5, 0, 5)),
    pytest.param((5, 5, 0)),
]


@pytest.mark.parametrize(
    "vals",
    tests,
)
@pytest.mark.redis_db
def test_rate_limit_failures(
    vals: Tuple[int, int, int], rate_limit_shards: Any
) -> None:
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


dual_write_tests = [
    pytest.param(None, None, None, None),
    pytest.param(None, None, None, 2),
    pytest.param(None, None, 2, None),
    pytest.param(None, None, 2, 2),
    pytest.param(None, 1, None, None),
    pytest.param(None, 1, None, 2),
    pytest.param(None, 1, 2, None),
    pytest.param(None, 1, 2, 2),
    pytest.param(1, None, None, None),
    pytest.param(1, None, None, 2),
    pytest.param(1, None, 2, None),
    pytest.param(1, None, 2, 2),
    pytest.param(1, 1, None, None),
    pytest.param(1, 1, None, 2),
    pytest.param(1, 1, 2, None),
    pytest.param(1, 1, 2, 2),
]


@pytest.mark.parametrize("ps_in_new, ps_in_old, ct_in_new, ct_in_old", dual_write_tests)
@pytest.mark.redis_db
def test_rate_limit_dual_write(
    ps_in_new: int | None, ps_in_old: int | None, ct_in_new: bool, ct_in_old: bool
) -> None:
    ps_key = "project_per_second_limit_36"
    ct_key = "project_concurrent_limit_36"

    if ps_in_new is not None:
        set_rate_limit_config(ps_key, ps_in_new, copy=False)
    if ct_in_new is not None:
        set_rate_limit_config(ct_key, ct_in_new, copy=False)
    if ps_in_old is not None:
        state.set_config(ps_key, ps_in_old)
    if ct_in_old is not None:
        state.set_config(ct_key, ct_in_old)

    ps_default, ct_default = 76, 77
    ps_found, ct_found = get_rate_limit_config(
        (ps_key, ps_default), (ct_key, ct_default)
    )

    expected_ps_saved = ps_in_old
    expected_ps_found = ps_in_old or ps_default
    expected_ct_saved = ct_in_old
    expected_ct_found = ct_in_old or ct_default

    assert ps_found == expected_ps_found
    assert ct_found == expected_ct_found

    assert (
        state.get_uncached_config(ps_key, config_key=state.rate_limit_config_key)
        == expected_ps_saved
    )
    assert (
        state.get_uncached_config(ct_key, config_key=state.rate_limit_config_key)
        == expected_ct_saved
    )


@pytest.mark.redis_db
def test_rate_limit_interface() -> None:
    ps_key = "project_per_second_limit_36"
    ct_key = "project_concurrent_limit_36"

    set_rate_limit_config(ps_key, 23)
    set_rate_limit_config(ct_key, 46)

    # Check that the keys are in new and old
    assert (
        state.get_uncached_config(ps_key, config_key=state.rate_limit_config_key) == 23
    )
    assert (
        state.get_uncached_config(ct_key, config_key=state.rate_limit_config_key) == 46
    )

    assert state.get_uncached_config(ps_key) == 23
    assert state.get_uncached_config(ct_key) == 46
