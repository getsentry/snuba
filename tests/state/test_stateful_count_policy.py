from datetime import datetime
from time import time

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.dead_letter_queue import InvalidMessage

from snuba.redis import redis_client
from snuba.state.stateful_count import StatefulCountInvalidMessagePolicy

NAME = "consumer-group"
SAVED_NAME = f"dlq:{NAME}"
INVALID_MESSAGE_EXCEPTION = InvalidMessage(
    Message(Partition(Topic(""), 0), 0, KafkaPayload(None, b"", []), datetime.now())
)


class TestStatefulCountPolicy:
    @pytest.fixture
    def policy(self) -> StatefulCountInvalidMessagePolicy:
        return StatefulCountInvalidMessagePolicy(
            consumer_group_name=NAME, limit=5, seconds=1
        )

    @pytest.fixture(autouse=True)
    def _clear_hash(self) -> None:
        redis_client.delete(SAVED_NAME)

    def test_simple(self, policy: StatefulCountInvalidMessagePolicy) -> None:
        now = int(time())
        policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        assert redis_client.hgetall(SAVED_NAME) == {str(now).encode("utf-8"): b"1"}

    def test_past_limit(self, policy: StatefulCountInvalidMessagePolicy,) -> None:
        now = int(time())
        # limit is 5, should raise on 6th invalid message
        for _ in range(5):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        assert redis_client.hgetall(SAVED_NAME) == {str(now).encode("utf-8"): b"5"}
        with pytest.raises(InvalidMessage):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        assert redis_client.hgetall(SAVED_NAME) == {str(now).encode("utf-8"): b"6"}

    def test_initialized_state(self) -> None:
        # initialize 4 hits
        now = int(time())
        p = redis_client.pipeline()
        p.hset(SAVED_NAME, str(now), 2)
        p.hset(SAVED_NAME, str(now - 1), 2)
        p.execute()
        # policy's init should pick load the state automatically from Redis
        policy: StatefulCountInvalidMessagePolicy = StatefulCountInvalidMessagePolicy(
            consumer_group_name=NAME, limit=5, seconds=2
        )
        # 5th invalid message shouldn't raise
        policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        # limit is 5, 6th should raise
        with pytest.raises(InvalidMessage):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        assert redis_client.hgetall(SAVED_NAME) == {
            str(now).encode("utf-8"): b"4",
            str(now - 1).encode("utf-8"): b"2",
        }

    def test_state_is_pruned(self, policy: StatefulCountInvalidMessagePolicy) -> None:
        # initialize 4 hits, but 2 are stale
        now = int(time())
        p = redis_client.pipeline()
        p.hset(SAVED_NAME, str(now), 2)
        p.hset(SAVED_NAME, str(now - 2), 2)
        p.execute()

        # should add a hit and remove 2 old ones
        policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)

        assert redis_client.hgetall(SAVED_NAME) == {str(now).encode("utf-8"): b"3"}
