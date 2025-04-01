import logging
from typing import Callable, Optional

from redis.exceptions import ConnectionError, ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError
from snuba import environment, settings
from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.state.cache.abstract import Cache, TValue
from snuba.utils.codecs import ExceptionAwareCodec
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "read_through_cache")


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2
SIMPLE_READTHROUGH = 3


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

    def __build_key(
        self, key: str, prefix: Optional[str] = None, suffix: Optional[str] = None
    ) -> str:
        return self.__prefix + "/".join(
            [bit for bit in [prefix, f"{{{key}}}", suffix] if bit is not None]
        )

    def get(self, key: str) -> Optional[TValue]:
        value = self.__client.get(self.__build_key(key))
        if value is None:
            return None

        return self.__codec.decode(value)

    def set(self, key: str, value: TValue) -> None:
        self.__client.set(
            self.__build_key(key),
            self.__codec.encode(value),
            ex=get_config("cache_expiry_sec", 1),
        )

    def __get_value_with_simple_readthrough(
        self,
        key: str,
        function: Callable[[], TValue],
        record_cache_hit_type: Callable[[int], None],
        timer: Optional[Timer] = None,
    ) -> TValue:
        record_cache_hit_type(SIMPLE_READTHROUGH)
        result_key = self.__build_key(key)

        cached_value = self.__client.get(result_key)
        if timer is not None:
            timer.mark("cache_get")
        metric_tags = timer.tags if timer is not None else {}

        if cached_value is not None:
            record_cache_hit_type(RESULT_VALUE)
            decoded_cache_value = self.__codec.decode(cached_value)
            if timer:
                timer.set_duration_between_marks(
                    "right_before_execute",
                    "execute",
                    decoded_cache_value["profile"]["elapsed"],
                )
            return decoded_cache_value
        else:
            try:
                value = function()
                self.__client.set(
                    result_key,
                    self.__codec.encode(value),
                    ex=get_config("cache_expiry_sec", 1),
                )
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
        timer: Optional[Timer] = None,
    ) -> TValue:
        # in case something is wrong with redis, we want to be able to
        # disable the read_through_cache but still serve traffic.
        if get_config("read_through_cache.short_circuit", 0):
            return function()

        try:
            # set disable_lua_scripts to use the simple read-through cache without queueing.
            return self.__get_value_with_simple_readthrough(
                key, function, record_cache_hit_type, timer
            )
        except (ConnectionError, ReadOnlyError, RedisTimeoutError, ValueError):
            if settings.RAISE_ON_READTHROUGH_CACHE_REDIS_FAILURES:
                raise
            metrics.increment("snuba.read_through_cache.fail_open")
            return function()
