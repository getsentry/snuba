import logging
import uuid
from typing import Callable, Optional, TypeVar

from pkg_resources import resource_string

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.state.cache.abstract import Cache, ExecutionError, ExecutionTimeoutError
from snuba.utils.codecs import Codec


T = TypeVar("T")


logger = logging.getLogger(__name__)


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2


class RedisCache(Cache[T]):
    def __init__(
        self, client: RedisClientType, prefix: str, codec: Codec[str, T]
    ) -> None:
        self.__client = client
        self.__prefix = prefix
        self.__codec = codec

        # TODO: This should probably be lazily instantiated, rather than
        # automatically happening at startup.
        self.__script_get = client.register_script(
            resource_string("snuba", "state/cache/redis/scripts/get.lua")
        )
        self.__script_set = client.register_script(
            resource_string("snuba", "state/cache/redis/scripts/set.lua")
        )

    def __build_key(self, key: str) -> str:
        return f"{self.__prefix}{key}"

    def get(self, key: str) -> Optional[T]:
        value = self.__client.get(self.__build_key(key))
        if value is None:
            return None

        return self.__codec.decode(value)

    def set(self, key: str, value: T) -> None:
        self.__client.set(
            self.__build_key(key),
            self.__codec.encode(value),
            ex=get_config("cache_expiry_sec", 1),
        )

    def get_or_execute(self, key: str, function: Callable[[], T], timeout: int) -> T:
        key = self.__build_key(key)

        result = self.__script_get(
            [key, f"{key}:q", f"{key}:u"], [timeout, uuid.uuid1().hex]
        )

        if result[0] == RESULT_VALUE:
            logger.debug("Immediately returning result from cache hit.")
            return self.__codec.decode(result[1])
        elif result[0] == RESULT_EXECUTE:
            task_id = result[1].decode("utf-8")
            logger.debug("Executing task (id: %r)...", task_id)
            keys = [key, f"{key}:q", f"{key}:u", f"{key}:n:{task_id}"]
            argv = [task_id, 60]
            try:
                value = function()
            except Exception:
                raise
            else:
                argv.extend(
                    [self.__codec.encode(value), get_config("cache_expiry_sec", 1)]
                )
            finally:
                logger.debug("Setting result and waking blocked clients...")
                self.__script_set(keys, argv)
            return value
        elif result[0] == RESULT_WAIT:
            task_id = result[1].decode("utf-8")
            timeout_remaining = result[2]
            effective_timeout = min(timeout_remaining, timeout)
            logger.debug(
                "Waiting for task result (id: %r) for up to %s seconds...",
                task_id,
                effective_timeout,
            )
            if not self.__client.blpop(f"{key}:n:{task_id}", effective_timeout):
                raise ExecutionTimeoutError("timed out waiting for result")
            else:
                value = self.__client.get(key)
                if value is None:
                    raise ExecutionError("no value at key")
                else:
                    return self.__codec.decode(value)
        else:
            raise ValueError("unexpected result from script")
