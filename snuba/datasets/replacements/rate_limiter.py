import time
from contextlib import contextmanager
from enum import Enum
from typing import Iterator, MutableMapping, Tuple

from snuba.state import get_config

RATE_LIMIT_PER_SEC = "mem_rate_limit_per_sec_"

# Stores the rate limit current state.
# The key is the bucket to keep multiple instances logically apart
# The value contains the epoch and the number of attempt in that
# epoch
BUCKETS: MutableMapping[str, Tuple[int, int]] = {}


class RateLimitResult(Enum):
    OFF = "off"
    WITHIN_QUOTA = "within_quota"
    THROTTLED = "throttled"


@contextmanager
def rate_limit(bucket: str) -> Iterator[Tuple[RateLimitResult, int]]:
    """
    A simple context manager that implements a per second window
    rate limiter that slows down operations to a maximum number
    of operations per second.

    Time is divided in one second long windows. This context can
    be acquired only a configurable number of times per window.
    Overflowing attempt are forced to wait for the next available
    window.

    This works only within a single process and is not thread safe.
    Do not use it concurrently.
    """

    limit = get_config(f"{RATE_LIMIT_PER_SEC}{bucket}", None)
    if not limit:
        yield (RateLimitResult.OFF, 0)
    else:
        current_time = time.time()
        current_epoch = int(current_time)
        current_bucket = BUCKETS.get(bucket)
        if current_bucket is None or current_epoch != current_bucket[0]:
            BUCKETS[bucket] = (current_epoch, 1)
            new_rate = 1
            state = RateLimitResult.WITHIN_QUOTA
        elif current_bucket[1] >= limit:
            new_epoch = current_epoch + 1
            time.sleep(new_epoch - current_time)
            BUCKETS[bucket] = (new_epoch, 1)
            new_rate = 1
            state = RateLimitResult.THROTTLED
        else:
            BUCKETS[bucket] = (current_epoch, current_bucket[1] + 1)
            new_rate = current_bucket[1] + 1
            state = RateLimitResult.WITHIN_QUOTA
        yield (state, new_rate)
