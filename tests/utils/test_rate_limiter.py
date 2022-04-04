from typing import Sequence, Tuple
from unittest.mock import Mock, patch

import pytest

from snuba.utils.rate_limiter import RateLimiter, RateLimitResult

test_cases = [
    pytest.param(
        [
            ("bucket1", 10.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket1", 10.01, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket within quota",
    ),
    pytest.param(
        [
            ("bucket2", 20.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket2", 20.01, RateLimitResult.WITHIN_QUOTA, 2),
            ("bucket2", 20.02, RateLimitResult.WITHIN_QUOTA, 3),
            ("bucket2", 20.03, RateLimitResult.THROTTLED, 1),
            ("bucket2", 21.01, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket throttled",
    ),
    pytest.param(
        [
            ("bucket3", 30.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket3", 31.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket3", 32.01, RateLimitResult.WITHIN_QUOTA, 1),
        ],
        id="Single bucket multiple windows",
    ),
]


@patch("time.sleep")
@pytest.mark.parametrize("trials", test_cases)
def test_rate_limiter(
    mock_sleep: Mock, trials: Sequence[Tuple[str, float, RateLimitResult, int]]
) -> None:
    rate_limiter = RateLimiter("bucket", 3.0)
    for bucket, time_resp, expected_result, expected_quota in trials:
        with patch("time.time", return_value=time_resp):
            # We need to pass the rate limit in since we are mocking time
            # When mocking time, get_config would be completely unreliable
            # and this would propagate across tests because get_config
            # memoizes the result in a way that cannot be reset between tests.
            with rate_limiter as quota:
                assert quota == (expected_result, expected_quota)
                if expected_result == RateLimitResult.THROTTLED:
                    mock_sleep.assert_called_once()


def test_disabled() -> None:
    rate_limiter = RateLimiter("bucket")
    with rate_limiter as quota:
        assert quota == (RateLimitResult.OFF, 0)
