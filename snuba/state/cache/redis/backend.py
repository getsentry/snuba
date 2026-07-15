import logging
from collections.abc import Callable

import sentry_sdk
from redis import ResponseError
from redis.exceptions import ConnectionError, ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from snuba import environment, settings
from snuba.redis import RedisClientType
from snuba.state.cache.abstract import Cache, TValue
from snuba.state.sentry_options import get_option
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "read_through_cache")


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2
SIMPLE_READTHROUGH = 3


class FuzzyMatchException:
    def __init__(self, exception: type[Exception], message: str | None = None):
        self._exception = exception
        self._message = message

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Exception):
            return isinstance(other, self._exception) and (
                self._message is None or self._message == str(other)
            )
        if isinstance(other, self.__class__):
            return other._exception == self._exception and other._message == self._message
        return False


DONT_CAPTURE_ERRORS = (
    # These errors are not actionable: they reflect transient redis/infra conditions
    # rather than bugs in Snuba, so we don't send them to Sentry. They are still
    # tracked via the datadog metrics snuba.read_through_cache.redis_cache_get_error
    # and snuba.read_through_cache.redis_cache_set_error.
    FuzzyMatchException(ResponseError, "OOM command not allowed under OOM prevention."),
    FuzzyMatchException(RedisTimeoutError),
    # Redis connection errors, e.g. "Connection reset by peer" (SNUBA-BQ6, SNUBA-B53).
    FuzzyMatchException(ConnectionError),
)


class RedisCache(Cache[TValue]):
    def __init__(
        self,
        client: RedisClientType,
        prefix: str,
        codec: ExceptionAwareCodec[bytes, TValue],
    ) -> None:
        self.__client = client
        self.__prefix = prefix
        self.__codec = codec

    def __build_key(self, key: str, prefix: str | None = None, suffix: str | None = None) -> str:
        return self.__prefix + "/".join(
            [bit for bit in [prefix, f"{{{key}}}", suffix] if bit is not None]
        )

    def get(self, key: str) -> TValue | None:
        value = self.__client.get(self.__build_key(key))
        if value is None:
            return None

        return self.__codec.decode(value)

    def set(self, key: str, value: TValue) -> None:
        self.__client.set(
            self.__build_key(key),
            self.__codec.encode(value),
            ex=get_option("cache_expiry_sec", 1),
        )

    def __get_value_with_simple_readthrough(
        self,
        key: str,
        function: Callable[[], TValue],
        record_cache_hit_type: Callable[[int], None],
        timer: Timer | None = None,
    ) -> TValue:
        record_cache_hit_type(SIMPLE_READTHROUGH)
        result_key = self.__build_key(key)
        metric_tags = (timer.tags if timer is not None else {}) or {}

        cached_value = None
        try:
            cached_value = self.__client.get(result_key)
        except Exception as e:
            if settings.RAISE_ON_READTHROUGH_CACHE_REDIS_FAILURES:
                raise e
            if e not in DONT_CAPTURE_ERRORS:
                sentry_sdk.capture_exception(e)
            metrics.increment("redis_cache_get_error", tags={"error": str(e), **metric_tags})

        if timer is not None:
            timer.mark("cache_get")

        if cached_value is not None:
            record_cache_hit_type(RESULT_VALUE)
            return self.__codec.decode(cached_value)
        try:
            value = function()
            try:
                self.__client.set(
                    result_key,
                    self.__codec.encode(value),
                    ex=get_option("cache_expiry_sec", 1),
                )
            except Exception as e:
                metrics.increment("redis_cache_set_error", tags={"error": str(e), **metric_tags})
                if e not in DONT_CAPTURE_ERRORS:
                    sentry_sdk.capture_exception(e)
                return value
            record_cache_hit_type(RESULT_EXECUTE)
            if timer is not None:
                timer.mark("cache_set")
        except Exception as e:
            metrics.increment("execute_error", tags=metric_tags)
            raise e
        return value

    def get_readthrough(
        self,
        key: str,
        function: Callable[[], TValue],
        record_cache_hit_type: Callable[[int], None],
        timer: Timer | None = None,
    ) -> TValue:
        # in case something is wrong with redis, we want to be able to
        # disable the read_through_cache but still serve traffic.
        if get_option("read_through_cache.short_circuit", False):
            return function()

        try:
            return self.__get_value_with_simple_readthrough(
                key, function, record_cache_hit_type, timer
            )
        except (ConnectionError, ReadOnlyError, RedisTimeoutError, ValueError):
            if settings.RAISE_ON_READTHROUGH_CACHE_REDIS_FAILURES:
                raise
            metrics.increment("snuba.read_through_cache.fail_open")
            return function()
