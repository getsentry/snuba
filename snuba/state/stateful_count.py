from time import time
from typing import Sequence, Tuple

from arroyo.processing.strategies.dead_letter_queue import (
    CountInvalidMessagePolicy,
    InvalidMessage,
)

from snuba.redis import redis_client


class StatefulCountInvalidMessagePolicy(CountInvalidMessagePolicy):
    """
    An extension of the CountInvalidMessagePolicy which is able to save and load
    the state of counted hits in Redis
    """

    def __init__(self, consumer_group_name: str, limit: int, seconds: int = 60) -> None:
        self.__name = f"dlq:{consumer_group_name}"
        self.__seconds = seconds
        super().__init__(limit, seconds, self._load_state())

    def handle_invalid_message(self, e: InvalidMessage) -> None:
        self._add_to_redis()
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

    def _prune(self, now: int, timestamps: Sequence[bytes]) -> None:
        """
        Removes old timestamps from the Redis hash
        """
        oldest_time = now - self.__seconds
        old_timestamps = [k for k in timestamps if int(k) < oldest_time]
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
