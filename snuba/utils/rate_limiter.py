from __future__ import annotations

import time
from enum import Enum
from threading import Lock
from typing import Any, Optional, Tuple

from snuba.state.sentry_options import get_mapped_float_option

# sentry-options dict option whose keys are rate-limit bucket names and whose
# values are the per-bucket max operations-per-second. Migrated from the
# per-bucket runtime config keys "mem_rate_limit_per_sec_<bucket>".
RATE_LIMIT_PER_SEC_OPTION = "mem_rate_limit_per_sec"


class RateLimitResult(Enum):
    OFF = "off"
    WITHIN_QUOTA = "within_quota"
    THROTTLED = "throttled"


class RateLimiter:
    """
    A simple context manager that implements a per second window
    rate limiter that slows down operations to a maximum number
    of operations per second.

    This works only within a single process but it is threadsafe.

    Time is divided in one second long windows. This context can
    be acquired only a configurable number of times per window.
    Overflowing attempts are forced to wait for the next available
    window.
    """

    def __init__(self, bucket: str, max_rate_per_sec: Optional[float] = None) -> None:
        self.__lock = Lock()
        self.__bucket_epoch: Optional[int] = None
        self.__bucket_attempts: Optional[int] = None
        self.__max_rate_per_sec = max_rate_per_sec
        self.__bucket = bucket

    def __enter__(self) -> Tuple[RateLimitResult, int]:
        limit = (
            get_mapped_float_option(RATE_LIMIT_PER_SEC_OPTION, self.__bucket, 0.0)
            if not self.__max_rate_per_sec
            else self.__max_rate_per_sec
        )

        if not limit:
            return (RateLimitResult.OFF, 0)
        with self.__lock:
            current_time = time.time()
            current_epoch = int(current_time)
            if (
                self.__bucket_epoch is None
                or self.__bucket_attempts is None
                or current_epoch != self.__bucket_epoch
            ):
                self.__bucket_epoch = current_epoch
                self.__bucket_attempts = 1
                ret_state = RateLimitResult.WITHIN_QUOTA
            elif self.__bucket_attempts >= limit:
                new_epoch = current_epoch + 1
                time.sleep(new_epoch - current_time)
                self.__bucket_epoch = new_epoch
                self.__bucket_attempts = 1
                ret_state = RateLimitResult.THROTTLED
            else:
                self.__bucket_epoch = current_epoch
                self.__bucket_attempts += 1
                ret_state = RateLimitResult.WITHIN_QUOTA

        return (ret_state, self.__bucket_attempts)

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        pass
