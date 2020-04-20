import logging
import uuid
from typing import Callable, Optional

from pkg_resources import resource_string
from redis.exceptions import ResponseError

from snuba.redis import RedisClientType
from snuba.state import get_config
from snuba.state.cache.abstract import (
    Cache,
    ExecutionError,
    ExecutionTimeoutError,
    TValue,
)
from snuba.utils.codecs import Codec


logger = logging.getLogger(__name__)


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2


class RedisCache(Cache[TValue]):
    def __init__(
        self, client: RedisClientType, prefix: str, codec: Codec[bytes, TValue]
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
        return f"{self.__prefix}{{{key}}}"

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

    def get_or_execute(
        self, key: str, function: Callable[[], TValue], timeout: int
    ) -> TValue:
        result_key = self.__build_key(key)
        wait_queue_key = f"{self.__build_key(key)}:w"
        task_ident_key = f"{self.__build_key(key)}:t"

        result = self.__script_get(
            [result_key, wait_queue_key, task_ident_key], [timeout, uuid.uuid1().hex]
        )

        if result[0] == RESULT_VALUE:
            logger.debug("Immediately returning result from cache hit.")
            return self.__codec.decode(result[1])
        elif result[0] == RESULT_EXECUTE:
            task_ident = result[1].decode("utf-8")
            task_timeout = int(result[2])
            logger.debug(
                "Executing task (%r) with %s second timeout...",
                task_ident,
                task_timeout,
            )

            argv = [task_ident, 60]
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
                try:
                    self.__script_set(
                        [
                            result_key,
                            wait_queue_key,
                            task_ident_key,
                            self.__build_notify_queue_key(key, task_ident),
                        ],
                        argv,
                    )
                except ResponseError:
                    logger.warning("Error setting cache result!", exc_info=True)
            return value
        elif result[0] == RESULT_WAIT:
            task_ident = result[1].decode("utf-8")
            task_timeout_remaining = int(result[2])

            # Block until an item to be available in the notify queue or we hit
            # one of the two possible timeouts.
            effective_timeout = min(task_timeout_remaining, timeout)
            logger.debug(
                "Waiting for task result (%r) for up to %s seconds...",
                task_ident,
                effective_timeout,
            )
            if not self.__client.blpop(
                self.__build_notify_queue_key(key, task_ident), effective_timeout
            ):
                if effective_timeout == task_timeout_remaining:
                    # If the effective timeout was the remaining task timeout,
                    # this means that the client responsible for generating the
                    # cache value didn't do so before it promised to.
                    raise ExecutionTimeoutError(
                        "result not available before execution deadline"
                    )
                else:
                    # If the effective timeout was the timeout provided to this
                    # method, that means that our timeout was stricter than the
                    # execution timeout.
                    raise TimeoutError("timed out waiting for result")
            else:
                # If we didn't timeout, there should be a value waiting for us.
                # If there is no value, that means that the client responsible
                # for generating the cache value errored while generating it.
                raw_value = self.__client.get(result_key)
                if raw_value is None:
                    # TODO: If we wanted to get clever, this could include the
                    # error message from the other client, or a Sentry ID or
                    # something.
                    raise ExecutionError("no value at key")
                else:
                    return self.__codec.decode(raw_value)
        else:
            raise ValueError("unexpected result from script")

    def __build_notify_queue_key(self, key: str, task_ident: str) -> str:
        return f"{self.__build_key(key)}:n:{task_ident}"
