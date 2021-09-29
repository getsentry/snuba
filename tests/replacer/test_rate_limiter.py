import time
from typing import Sequence, Tuple
from unittest.mock import Mock, patch

import pytest

from snuba import state
from snuba.datasets.replacements.rate_limiter import (
    RATE_LIMIT_PER_SEC,
    RateLimitResult,
    rate_limit,
)

test_cases = [
    pytest.param(
        [
            ("bucket1", 100.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket1", 100.01, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket within quota",
    ),
    pytest.param(
        [
            ("bucket2", 100.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket2", 100.01, RateLimitResult.WITHIN_QUOTA, 2),
            ("bucket2", 100.02, RateLimitResult.WITHIN_QUOTA, 3),
            ("bucket2", 100.03, RateLimitResult.THROTTLED, 1),
            ("bucket2", 101.01, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket throttled",
    ),
    pytest.param(
        [
            ("bucket3", 100.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket3", 101.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket3", 102.01, RateLimitResult.WITHIN_QUOTA, 1),
        ],
        id="Single bucket multiple windows",
    ),
    pytest.param(
        [
            ("bucket4", 100.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket5", 100.02, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket4", 100.02, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Single bucket multiple windows",
    ),
    pytest.param(
        [
            ("bucket6", 100.01, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket6", 100.01, RateLimitResult.WITHIN_QUOTA, 2),
            ("bucket6", 100.02, RateLimitResult.WITHIN_QUOTA, 3),
            ("bucket7", 100.03, RateLimitResult.WITHIN_QUOTA, 1),
            ("bucket7", 100.04, RateLimitResult.WITHIN_QUOTA, 2),
        ],
        id="Multiple buckets",
    ),
]


@patch("time.sleep")
@pytest.mark.parametrize("trials", test_cases)
def test_rate_limiter(
    mock_sleep, trials: Sequence[Tuple[str, float, RateLimitResult, int]]
) -> None:
    # We need to set all the config values before getting one because
    # config is cached during read.
    for i in range(1, 8):
        state.set_config(f"{RATE_LIMIT_PER_SEC}bucket{i}", 3.0)

    for bucket, time_resp, expected_result, expected_quota in trials:
        time.time = Mock(return_value=time_resp)
        with rate_limit(bucket) as quota:
            assert quota == (expected_result, expected_quota)
            if expected_result == RateLimitResult.THROTTLED:
                pass
                mock_sleep.assert_called_once()
