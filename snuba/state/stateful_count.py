from threading import Thread
from time import time
from typing import Any, Sequence, Tuple

from arroyo.processing.strategies.dead_letter_queue import CountInvalidMessagePolicy
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    InvalidMessage,
)
from arroyo.types import TPayload

from snuba.redis import redis_client


class StatefulCountInvalidMessagePolicy(CountInvalidMessagePolicy[TPayload]):  # type: ignore
    """
    An extension of the CountInvalidMessagePolicy which is able to save and load
    the state of counted hits in Redis
    """

    def __init__(self, redis_hash_name: str, limit: int, seconds: int = 60) -> None:
        self.__name = redis_hash_name
        self.__seconds = seconds
        super().__init__(limit, seconds, self._load_state())

    def handle_invalid_message(self, e: InvalidMessage) -> None:
        """
        Asynchronously updates Redis hash with a new hit while
        the base Policy handles the invalid message
        """
        Thread(target=self._add_to_redis).start()
        super().handle_invalid_message(e)

    def _add_to_redis(self) -> None:
        """
        Increments the current time entry in Redis by 1
        """
        now = int(time())
        p = redis_client.pipeline()
        p.hincrby(self.__name, str(now), 1)
        p.hkeys(self.__name)
        self._prune(now, p.execute()[1])

    def _prune(self, now: int, timestamps: Sequence[Any]) -> None:
        """
        Removes old timestamps from the Redis hash
        """
        oldest_time = str(now - self.__seconds).encode("UTF-8")
        old_timestamps = [k for k in timestamps if k < oldest_time]
        if old_timestamps:
            redis_client.hdel(self.__name, *old_timestamps)

    def _load_state(self) -> Sequence[Tuple[int, int]]:
        """
        Retrieves saved state from Redis and returns a sorted timeseries
        of hits
        """
        hit_data = redis_client.hgetall(self.__name)
        state = [(int(timestamp), int(hits)) for (timestamp, hits) in hit_data.items()]
        state.sort(key=lambda entry: entry[0])
        return state
