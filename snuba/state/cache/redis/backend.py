import logging
import uuid
from typing import Callable, Optional

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
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
from snuba.utils.metrics.timer import Timer


logger = logging.getLogger(__name__)


RESULT_VALUE = 0
RESULT_EXECUTE = 1
RESULT_WAIT = 2


class RedisCache(Cache[TValue]):
    def __init__(
        self,
        client: RedisClientType,
        prefix: str,
        codec: Codec[bytes, TValue],
        executor: ThreadPoolExecutor,
    ) -> None:
        self.__client = client
        self.__prefix = prefix
        self.__codec = codec
        self.__executor = executor

        # TODO: This should probably be lazily instantiated, rather than
        # automatically happening at startup.
        self.__script_get = client.register_script(
            resource_string("snuba", "state/cache/redis/scripts/get.lua")
        )
        self.__script_set = client.register_script(
            resource_string("snuba", "state/cache/redis/scripts/set.lua")
        )

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

    def get_readthrough(
        self,
        key: str,
        function: Callable[[], TValue],
        timeout: int,
        timer: Optional[Timer] = None,
    ) -> TValue:
        # This method is designed with the following goals in mind:
        # 1. The value generation function is only executed when no value
        # already exists for the key.
        # 2. Only one client can execute the value generation function at a
        # time (up to a deadline, at which point the client is assumed to be
        # dead and its results are no longer valid.)
        # 3. The other clients waiting for the result of the value generation
        # function receive a result as soon as it is available.
        # 4. This remains compatible with the existing get/set API (at least
        # for the time being.)

        # This method shares the same keyspace as the conventional get and set
        # methods, which restricts this key to only containing the cache value
        # (or lack thereof.)
        result_key = self.__build_key(key)

        # The wait queue (a Redis list) is used to identify clients that are
        # currently "subscribed" to the evaluation of the function and awaiting
        # its result. The first member of this queue is a special case -- it is
        # the client responsible for executing the function and notifying the
        # subscribed clients of its completion. Only one wait queue should be
        # associated with a cache key at any time.
        wait_queue_key = self.__build_key(key, "tasks", "wait")

        # The task identity (a Redis bytestring) is used to store a unique
        # identifier for a single task evaluation and notification cycle. Only
        # one task should be associated with a cache key at any time.
        task_ident_key = self.__build_key(key, "tasks")

        # The notify queue (a Redis list) is used to unblock clients that are
        # waiting for the task to complete. **This implementation requires that
        # the number of clients waiting for responses via the notify queue is
        # no greater than the number of clients in the wait queue** (minus one
        # client, who is doing the work and not waiting.) If there are more
        # clients waiting for notifications than the members of the wait queue
        # (for any reason), some set of clients will never be notified. To be
        # safe and ensure that each client only waits for notifications from
        # tasks where it was also a member of the wait queue, the notify queue
        # includes the unique task identity as part of it's key.
        def build_notify_queue_key(task_ident: str) -> str:
            return self.__build_key(key, "tasks", f"notify/{task_ident}")

        # At this point, we have all of the information we need to figure out
        # if the key exists, and if it doesn't, if we should start working or
        # wait for a different client to finish the work. We have to pass the
        # task creation parameters -- the timeout (execution deadline) and a
        # new task identity just in case we are the first in line.
        result = self.__script_get(
            [result_key, wait_queue_key, task_ident_key], [timeout, uuid.uuid1().hex]
        )

        if timer is not None:
            timer.mark("cache_get")

        if result[0] == RESULT_VALUE:
            # If we got a cache hit, this is easy -- we just return it.
            logger.debug("Immediately returning result from cache hit.")
            return self.__codec.decode(result[1])
        elif result[0] == RESULT_EXECUTE:
            # If we were the first in line, we need to execute the function.
            # We'll also get back the task identity to use for sending
            # notifications and approximately how long we have to run the
            # function. (In practice, these should be the same values as what
            # we provided earlier.)
            task_ident = result[1].decode("utf-8")
            task_timeout = int(result[2])
            logger.debug(
                "Executing task (%r) with %s second timeout...",
                task_ident,
                task_timeout,
            )

            argv = [task_ident, 60]
            try:
                # The task is run in a thread pool so that we can return
                # control to the caller once the timeout is reached.
                value = self.__executor.submit(function).result(task_timeout)
                argv.extend(
                    [self.__codec.encode(value), get_config("cache_expiry_sec", 1)]
                )
            except concurrent.futures.TimeoutError as error:
                raise TimeoutError("timed out waiting for value") from error
            finally:
                # Regardless of whether the function succeeded or failed, we
                # need to mark the task as completed. If there is no result
                # value, other clients will know that we raised an exception.
                logger.debug("Setting result and waking blocked clients...")
                try:
                    self.__script_set(
                        [
                            result_key,
                            wait_queue_key,
                            task_ident_key,
                            build_notify_queue_key(task_ident),
                        ],
                        argv,
                    )
                except ResponseError:
                    # An error response here indicates that we overran our
                    # deadline, or there was some other issue when trying to
                    # put the result value in the cache. This doesn't affect
                    # _our_ evaluation of the task, so log it and move on.
                    logger.warning("Error setting cache result!", exc_info=True)
                else:
                    if timer is not None:
                        timer.mark("cache_set")
            return value
        elif result[0] == RESULT_WAIT:
            # If we were not the first in line, we need to wait for the first
            # client to finish and populate the cache with the result value.
            # We use the provided task identity to figure out where to listen
            # for notifications, and the task timeout remaining informs us the
            # maximum amount of time that we should expect to wait.
            task_ident = result[1].decode("utf-8")
            task_timeout_remaining = int(result[2])
            effective_timeout = min(task_timeout_remaining, timeout)
            logger.debug(
                "Waiting for task result (%r) for up to %s seconds...",
                task_ident,
                effective_timeout,
            )

            notification_received = (
                self.__client.blpop(
                    build_notify_queue_key(task_ident), effective_timeout
                )
                is not None
            )

            if timer is not None:
                timer.mark("dedupe_wait")

            if notification_received:
                # There should be a value waiting for us at the result key.
                raw_value = self.__client.get(result_key)

                # If there is no value, that means that the client responsible
                # for generating the cache value errored while generating it.
                if raw_value is None:
                    # TODO: If we wanted to get clever, this could include the
                    # error message from the other client, or a Sentry ID or
                    # something.
                    raise ExecutionError("no value at key")
                else:
                    return self.__codec.decode(raw_value)
            else:
                # We timed out waiting for the notification -- something went
                # wrong with the client that was generating the cache value.
                if effective_timeout == task_timeout_remaining:
                    # If the effective timeout was the remaining task timeout,
                    # this means that the client responsible for generating the
                    # cache value didn't do so before it promised to.
                    raise ExecutionTimeoutError(
                        "result not available before execution deadline"
                    )
                else:
                    # If the effective timeout was the timeout provided to this
                    # method, that means that our timeout was stricter
                    # (smaller) than the execution timeout. The other client
                    # may still be working, but we're not waiting.
                    raise TimeoutError("timed out waiting for result")
        else:
            raise ValueError("unexpected result from script")
