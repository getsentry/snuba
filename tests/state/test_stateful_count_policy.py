import time
from datetime import datetime
from typing import MutableSequence, Optional

import pytest
from arroyo.backends.kafka import KafkaPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.dead_letter_queue import (
    DeadLetterQueue,
    IgnoreInvalidMessagePolicy,
    InvalidKafkaMessage,
    InvalidMessage,
    InvalidMessages,
)
from arroyo.types import Message, Partition, Topic

from snuba.redis import RedisClientKey, get_redis_client
from snuba.state.stateful_count import StatefulCountInvalidMessagePolicy

redis_client = get_redis_client(RedisClientKey.DLQ)

CONSUMER_GROUP_NAME = "test-consumer-group"
REDIS_KEY = f"dlq:{CONSUMER_GROUP_NAME}"
NO_KEY = "No key"
BAD_PAYLOAD = "Bad payload"


def kafka_message_to_invalid_kafka_message(
    message: Message[KafkaPayload], reason: str
) -> InvalidKafkaMessage:
    return InvalidKafkaMessage(
        payload=message.payload.value,
        timestamp=message.timestamp,
        topic=message.partition.topic.name,
        consumer_group=CONSUMER_GROUP_NAME,
        partition=message.partition.index,
        offset=message.offset,
        headers=message.payload.headers,
        reason=reason,
    )


class FakeProcessingStep(ProcessingStrategy[KafkaPayload]):
    """
    Raises InvalidMessages if a submitted message has no key in payload.
    """

    def poll(self) -> None:
        raise InvalidMessages(
            [
                InvalidKafkaMessage(
                    payload=b"a bad message",
                    timestamp=datetime.now(),
                    topic="",
                    consumer_group=CONSUMER_GROUP_NAME,
                    partition=0,
                    offset=0,
                    headers=[],
                    reason=NO_KEY,
                )
            ]
        )

    def join(self, timeout: Optional[float] = None) -> None:
        pass

    def terminate(self) -> None:
        pass

    def close(self) -> None:
        pass

    def submit(self, message: Message[KafkaPayload]) -> None:
        """
        Valid message is one with a key and decodable value.
        """
        reason: str = ""

        try:
            message.payload.value.decode("utf-8")
        except UnicodeDecodeError:
            reason = BAD_PAYLOAD
        else:
            if message.payload.key is None:
                reason = NO_KEY

        if reason:
            raise InvalidMessages(
                [kafka_message_to_invalid_kafka_message(message, reason)]
            )


class FakeBatchingProcessingStep(FakeProcessingStep):
    """
    Batches up to 5 messages.
    """

    def __init__(self) -> None:
        self._batch: MutableSequence[Message[KafkaPayload]] = []

    def submit(self, message: Message[KafkaPayload]) -> None:
        self._batch.append(message)
        if len(self._batch) > 4:
            self._submit_multiple()

    def _process_message(self, message: Message[KafkaPayload]) -> None:
        """
        Some processing we want to happen per message.
        """
        if message.payload.key is None:
            raise InvalidMessages(
                [kafka_message_to_invalid_kafka_message(message, NO_KEY)]
            )

    def _submit_multiple(self) -> None:
        """
        Valid message is one with a key.
        """
        bad_messages: MutableSequence[InvalidMessage] = []
        for message in self._batch:
            try:
                self._process_message(message)
            except InvalidMessages as e:
                bad_messages += e.messages
        # At this point, we have some bad messages but the
        # good ones have been processed without failing entire batch
        self._batch = []
        if bad_messages:
            raise InvalidMessages(bad_messages)


@pytest.fixture
def processing_step() -> ProcessingStrategy[KafkaPayload]:
    return FakeProcessingStep()


@pytest.fixture
def valid_message() -> Message[KafkaPayload]:
    valid_payload = KafkaPayload(b"", b"", [])
    return Message(Partition(Topic(""), 0), 0, valid_payload, datetime.now())


@pytest.fixture
def invalid_message() -> Message[KafkaPayload]:
    invalid_payload = KafkaPayload(None, b"", [])
    return Message(Partition(Topic(""), 0), 0, invalid_payload, datetime.now())


def test_count(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step,
        StatefulCountInvalidMessagePolicy(
            CONSUMER_GROUP_NAME, next_policy=IgnoreInvalidMessagePolicy(), limit=5
        ),
    )
    dlq_count.submit(valid_message)
    for _ in range(5):
        dlq_count.submit(invalid_message)
    with pytest.raises(InvalidMessages):
        dlq_count.submit(invalid_message)


def test_count_short(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:
    dlq_count_short: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step,
        StatefulCountInvalidMessagePolicy(
            CONSUMER_GROUP_NAME,
            next_policy=IgnoreInvalidMessagePolicy(),
            limit=1,
            seconds=1,
        ),
    )
    dlq_count_short.submit(valid_message)
    dlq_count_short.submit(invalid_message)
    with pytest.raises(InvalidMessages):
        dlq_count_short.submit(invalid_message)
    time.sleep(1)
    dlq_count_short.submit(invalid_message)


def test_stateful_count(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
    processing_step: FakeProcessingStep,
) -> None:

    now = int(datetime.now().timestamp())

    p = redis_client.pipeline()
    p.hset(REDIS_KEY, str(now - 1), 2)
    p.hset(REDIS_KEY, str(now), 2)
    p.execute()

    # Stateful count DLQ intialized with 4 hits in the state
    dlq_count_load_state: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        processing_step,
        StatefulCountInvalidMessagePolicy(
            consumer_group_name=CONSUMER_GROUP_NAME,
            next_policy=IgnoreInvalidMessagePolicy(),
            limit=5,
        ),
    )

    dlq_count_load_state.submit(valid_message)

    # Limit is 5, 4 hits exist, 1 more should be added without exception raised
    dlq_count_load_state.submit(invalid_message)

    # Limit is 5, 5 hits exist, next invalid message should cause exception
    with pytest.raises(InvalidMessages):
        dlq_count_load_state.submit(invalid_message)


def test_invalid_batched_messages(
    valid_message: Message[KafkaPayload],
    invalid_message: Message[KafkaPayload],
) -> None:
    fake_batching_processor = FakeBatchingProcessingStep()
    count_policy = StatefulCountInvalidMessagePolicy(
        CONSUMER_GROUP_NAME, next_policy=IgnoreInvalidMessagePolicy(), limit=5
    )
    dlq_count: DeadLetterQueue[KafkaPayload] = DeadLetterQueue(
        fake_batching_processor, count_policy
    )

    """
    Batch submits on 5th message, count policy raises on 6th invalid message processed.

    First batch submitted on 3rd iteration with 3 invalid and 2 valid messages
    - count policy now holds 3 invalid messages

    Second batch submitted on 5th iteration with 3 valid and and 2 invalid messages
    - count policy now holds 5 invalid messages
    """
    for _ in range(5):
        dlq_count.submit(invalid_message)
        dlq_count.submit(valid_message)

    # build the next batch with 4 invalid messages, count policy still only sees 5 invalid messages
    for _ in range(4):
        dlq_count.submit(invalid_message)
    assert count_policy._count() == 5

    """
    Next message submitted triggers batch to submit
    - submits 4 batched invalid messages to the count policy, triggering it to raise
    """
    with pytest.raises(InvalidMessages) as e_info:
        dlq_count.submit(valid_message)

    assert len(e_info.value.messages) == 4
    assert count_policy._count() == 9
