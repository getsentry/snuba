from typing import Sequence, Tuple
from unittest.mock import patch

import pytest

from snuba.datasets.replacements.rate_limiter import RateLimitResult, rate_limit

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
    pytest.param(
        [
            ("bucket4", 40.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket5", 40.02, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket4", 40.02, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket multiple windows",
    ),
    pytest.param(
        [
            ("bucket6", 50.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket6", 50.01, RateLimitResult.WITHIN_QUOTA, 2),
            ("bucket6", 50.02, RateLimitResult.WITHIN_QUOTA, 3),
            ("bucket7", 50.03, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket7", 50.04, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Multiple buckets",
    ),
]


@patch("time.sleep")
@pytest.mark.parametrize("trials", test_cases)
def test_rate_limiter(
    mock_sleep, trials: Sequence[Tuple[str, float, RateLimitResult, int]]
) -> None:
    for bucket, time_resp, expected_result, expected_quota in trials:
        with patch("time.time", return_value=time_resp):
            # We need to pass the rate limit in since we are mocking time
            # When mocking time, get_config would be completely unreliable
            # and this would propagate across tests because get_config
            # memoizes the result in a way that cannot be reset between tests.
            with rate_limit(bucket, 3.0) as quota:
                assert quota == (expected_result, expected_quota)
                if expected_result == RateLimitResult.THROTTLED:
                    mock_sleep.assert_called_once()


def test_disabled() -> None:
    with rate_limit("test") as quota:
        assert quota == (RateLimitResult.OFF, 0)
