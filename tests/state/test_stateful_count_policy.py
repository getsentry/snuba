import time
from datetime import datetime

import pytest
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.dead_letter_queue.policies.abstract import (
    InvalidMessage,
)

from snuba.redis import redis_client
from snuba.state.stateful_count import StatefulCountInvalidMessagePolicy

NAME = "test-stateful-count-policy"
INVALID_MESSAGE_EXCEPTION = InvalidMessage(
    Message(Partition(Topic(""), 0), 0, KafkaPayload(None, b"", []), datetime.now())
)


class TestStatefulCountPolicy:
    @pytest.fixture
    def policy(self) -> StatefulCountInvalidMessagePolicy[KafkaPayload]:
        return StatefulCountInvalidMessagePolicy(
            redis_hash_name=NAME, limit=5, seconds=1
        )

    @pytest.fixture(autouse=True)
    def _clear_hash(self) -> None:
        redis_client.delete(NAME)

    def test_simple(
        self, policy: StatefulCountInvalidMessagePolicy[KafkaPayload]
    ) -> None:
        now = int(time.time())
        policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        # waiting for thread
        time.sleep(0.1)
        assert redis_client.hgetall(NAME) == {str(now).encode("utf-8"): b"1"}

    def test_past_limit(
        self, policy: StatefulCountInvalidMessagePolicy[KafkaPayload],
    ) -> None:
        # limit is 5, should raise on 6th invalid message
        for _ in range(5):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        with pytest.raises(InvalidMessage):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)

    def test_initialized_state(self) -> None:
        # initialize 4 hits
        now = int(time.time())
        p = redis_client.pipeline()
        p.hset(NAME, str(now), 2)
        p.hset(NAME, str(now - 1), 2)
        p.execute()
        # policy's init should pick load the state automatically from Redis
        policy: StatefulCountInvalidMessagePolicy[
            KafkaPayload
        ] = StatefulCountInvalidMessagePolicy(redis_hash_name=NAME, limit=5, seconds=2)
        # 5th invalid message shouldn't raise
        policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        # limit is 5, 6th should raise
        with pytest.raises(InvalidMessage):
            policy.handle_invalid_message(INVALID_MESSAGE_EXCEPTION)
        # waiting for thread
        time.sleep(0.1)
        assert redis_client.hgetall(NAME) == {
            str(now).encode("UTF-8"): b"4",
            str(now - 1).encode("UTF-8"): b"2",
        }
