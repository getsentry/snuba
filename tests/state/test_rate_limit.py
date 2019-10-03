import pytest
from unittest.mock import patch
import uuid

from base import BaseTest
from snuba import state
from snuba.state.rate_limit import RateLimitAggregator, RateLimitExceeded, RateLimitParameters


class TestRateLimit(BaseTest):
    def test_concurrent_limit(self):
        # No concurrent limit should not raise
        rate_limit_params = RateLimitParameters('foo', 'bar', None, None)
        with RateLimitAggregator([rate_limit_params]):
            pass

        # 0 concurrent limit
        rate_limit_params = RateLimitParameters('foo', 'bar', None, 0)

        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator([rate_limit_params]):
                pass

        # Concurrent limit 1 with consecutive queries should not raise
        rate_limit_params = RateLimitParameters('foo', 'bar', None, 1)

        with RateLimitAggregator([rate_limit_params]):
            pass

        with RateLimitAggregator([rate_limit_params]):
            pass

        # # Concurrent limit with concurrent queries
        rate_limit_params = RateLimitParameters('foo', 'bar', None, 1)

        with pytest.raises(RateLimitExceeded):
            with RateLimitAggregator([rate_limit_params]):
                with RateLimitAggregator([rate_limit_params]):
                    pass

        # Concurrent with different buckets should not raise
        rate_limit_params1 = RateLimitParameters('foo', 'bar', None, 1)
        rate_limit_params2 = RateLimitParameters('shoe', 'star', None, 1)

        with RateLimitAggregator([rate_limit_params1]):
            with RateLimitAggregator([rate_limit_params2]):
                pass

    def test_per_second_limit(self):
        bucket = uuid.uuid4()
        rate_limit_params = RateLimitParameters('foo', bucket, 1, None)
        # Create 30 queries at time 0, should all be allowed
        with patch.object(state.time, 'time', lambda: 0):
            for _ in range(30):
                with RateLimitAggregator([rate_limit_params]):
                    pass

        # Create another 30 queries at time 30, should also be allowed
        with patch.object(state.time, 'time', lambda: 30):
            for _ in range(30):
                with RateLimitAggregator([rate_limit_params]):
                    pass

        with patch.object(state.time, 'time', lambda: 60):
            # 1 more query should be allowed at T60 because it does not make the previous
            # rate exceed 1/sec until it has finished.
            with RateLimitAggregator([rate_limit_params]):
                pass

            # But the next one should not be allowed
            with pytest.raises(RateLimitExceeded):
                with RateLimitAggregator([rate_limit_params]):
                    pass

        # Another query at time 61 should be allowed because the first 30 queries
        # have fallen out of the lookback window
        with patch.object(state.time, 'time', lambda: 61):
            with RateLimitAggregator([rate_limit_params]):
                pass
