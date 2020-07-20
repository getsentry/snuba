import time
from contextlib import closing
from threading import Event
from typing import Optional, TypeVar

import pytest

from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.dummy import DummyBroker, DummyConsumer
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.synchronized import Commit, SynchronizedConsumer, commit_codec
from snuba.utils.streams.types import Message, Partition, Topic
from tests.assertions import assert_changes, assert_does_not_change


T = TypeVar("T")


def wait_for_consumer(consumer: Consumer[T], message: Message[T], attempts: int = 10):
    """Block until the provided consumer has received the provided message."""
    for i in range(attempts):
        part = consumer.tell().get(message.partition)
        if part is not None and part >= message.get_next_offset():
            return

        time.sleep(0.1)

    raise Exception(
        f"{message} was not received by {consumer} within {attempts} attempts"
    )


def test_synchronized_consumer(broker: DummyBroker[KafkaPayload]) -> None:
    topic = Topic("topic")
    commit_log_topic = Topic("commit-log")

    broker.create_topic(topic, partitions=1)
    broker.create_topic(commit_log_topic, partitions=1)

    consumer = broker.get_consumer("consumer")
    producer = broker.get_producer()
    commit_log_consumer = broker.get_consumer("commit-log-consumer")

    messages = [
        producer.produce(topic, KafkaPayload(None, f"{i}".encode("utf8"))).result(1.0)
        for i in range(6)
    ]

    synchronized_consumer: Consumer[KafkaPayload] = SynchronizedConsumer(
        consumer,
        commit_log_consumer,
        commit_log_topic=commit_log_topic,
        commit_log_groups={"leader-a", "leader-b"},
    )

    with closing(synchronized_consumer):
        synchronized_consumer.subscribe([topic])

        # The consumer should not consume any messages until it receives a
        # commit from both groups that are being followed.
        # TODO: This test is not ideal -- there are no guarantees that the
        # commit log worker has subscribed and started polling yet.
        with assert_changes(
            consumer.paused, [], [Partition(topic, 0)]
        ), assert_does_not_change(
            consumer.tell, {Partition(topic, 0): messages[0].offset}
        ):
            assert synchronized_consumer.poll(0.0) is None

        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit(
                        "leader-a", Partition(topic, 0), messages[0].get_next_offset()
                    )
                ),
            ).result(),
        )

        # The consumer should remain paused, since it needs both groups to
        # advance before it may continue.
        with assert_does_not_change(
            consumer.paused, [Partition(topic, 0)]
        ), assert_does_not_change(
            consumer.tell, {Partition(topic, 0): messages[0].offset}
        ):
            assert synchronized_consumer.poll(0.0) is None

        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit(
                        "leader-b", Partition(topic, 0), messages[0].get_next_offset()
                    )
                ),
            ).result(),
        )

        # The consumer should be able to resume consuming, since both consumers
        # have processed the first message.
        with assert_changes(consumer.paused, [Partition(topic, 0)], []), assert_changes(
            consumer.tell,
            {Partition(topic, 0): messages[0].offset},
            {Partition(topic, 0): messages[0].get_next_offset()},
        ):
            assert synchronized_consumer.poll(0.0) == messages[0]

        # After consuming the one available message, the consumer should be
        # paused again until the remote offsets advance.
        with assert_changes(
            consumer.paused, [], [Partition(topic, 0)]
        ), assert_does_not_change(
            consumer.tell, {Partition(topic, 0): messages[1].offset}
        ):
            assert synchronized_consumer.poll(0.0) is None

        # Emulate the unlikely (but possible) scenario of the leader offsets
        # being within a series of compacted (deleted) messages by:
        # 1. moving the remote offsets forward, so that the partition is resumed
        # 2. seeking the consumer beyond the remote offsets

        producer.produce(
            commit_log_topic,
            commit_codec.encode(
                Commit("leader-a", Partition(topic, 0), messages[3].offset)
            ),
        ).result()

        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit("leader-b", Partition(topic, 0), messages[5].offset)
                ),
            ).result(),
        )

        # The consumer should be able to resume consuming, since both consumers
        # have processed the first message.
        with assert_changes(consumer.paused, [Partition(topic, 0)], []), assert_changes(
            consumer.tell,
            {Partition(topic, 0): messages[1].offset},
            {Partition(topic, 0): messages[1].get_next_offset()},
        ):
            assert synchronized_consumer.poll(0.0) == messages[1]

        # At this point, we manually seek the consumer offset, to emulate messages being skipped.
        with assert_changes(
            consumer.tell,
            {Partition(topic, 0): messages[2].offset},
            {Partition(topic, 0): messages[4].offset},
        ):
            consumer.seek({Partition(topic, 0): messages[4].offset})

        # Since the (effective) remote offset is the offset for message #3 (via
        # ``leader-a``), and the local offset is the offset of message #4, when
        # message #4 is consumed, it should be discarded and the offset should
        # be rolled back to wait for the commit log to advance.
        with assert_changes(
            consumer.paused, [], [Partition(topic, 0)]
        ), assert_does_not_change(
            consumer.tell, {Partition(topic, 0): messages[4].offset}
        ):
            assert synchronized_consumer.poll(0.0) is None

        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit("leader-a", Partition(topic, 0), messages[5].offset)
                ),
            ).result(),
        )

        # The consumer should be able to resume consuming.
        with assert_changes(consumer.paused, [Partition(topic, 0)], []), assert_changes(
            consumer.tell,
            {Partition(topic, 0): messages[4].offset},
            {Partition(topic, 0): messages[4].get_next_offset()},
        ):
            assert synchronized_consumer.poll(0.0) == messages[4]


def test_synchronized_consumer_pause_resume(broker: DummyBroker[KafkaPayload]) -> None:
    topic = Topic("topic")
    commit_log_topic = Topic("commit-log")

    broker.create_topic(topic, partitions=1)
    broker.create_topic(commit_log_topic, partitions=1)

    consumer = broker.get_consumer("consumer")
    producer = broker.get_producer()
    commit_log_consumer = broker.get_consumer("commit-log-consumer")

    messages = [
        producer.produce(topic, KafkaPayload(None, f"{i}".encode("utf8"))).result(1.0)
        for i in range(2)
    ]

    synchronized_consumer: Consumer[KafkaPayload] = SynchronizedConsumer(
        consumer,
        commit_log_consumer,
        commit_log_topic=commit_log_topic,
        commit_log_groups={"leader"},
    )

    with closing(synchronized_consumer):
        synchronized_consumer.subscribe([topic])

        # TODO: This test is not ideal -- there are no guarantees that the
        # commit log worker has subscribed and started polling yet.
        with assert_changes(
            synchronized_consumer.paused, [], [Partition(topic, 0)]
        ), assert_changes(consumer.paused, [], [Partition(topic, 0)]):
            synchronized_consumer.pause([Partition(topic, 0)])

        # Advancing the commit log offset should not cause the consumer to
        # resume, since it has been explicitly paused.
        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit("leader", Partition(topic, 0), messages[0].get_next_offset())
                ),
            ).result(),
        )

        with assert_does_not_change(consumer.paused, [Partition(topic, 0)]):
            assert synchronized_consumer.poll(0) is None

        # Resuming the partition does not immediately cause the partition to
        # resume, but it should look as if it is resumed to the caller.
        with assert_changes(
            synchronized_consumer.paused, [Partition(topic, 0)], []
        ), assert_does_not_change(consumer.paused, [Partition(topic, 0)]):
            synchronized_consumer.resume([Partition(topic, 0)])

        # The partition should be resumed on the next poll call, however.
        with assert_changes(consumer.paused, [Partition(topic, 0)], []):
            assert synchronized_consumer.poll(0) == messages[0]

        # Pausing due to hitting the offset fence should not appear as a paused
        # partition to the caller.
        with assert_does_not_change(synchronized_consumer.paused, []), assert_changes(
            consumer.paused, [], [Partition(topic, 0)]
        ):
            assert synchronized_consumer.poll(0) is None

        # Other pause and resume actions should not cause the inner consumer to
        # change its state while up against the fence.
        with assert_changes(
            synchronized_consumer.paused, [], [Partition(topic, 0)]
        ), assert_does_not_change(consumer.paused, [Partition(topic, 0)]):
            synchronized_consumer.pause([Partition(topic, 0)])

        with assert_changes(
            synchronized_consumer.paused, [Partition(topic, 0)], []
        ), assert_does_not_change(consumer.paused, [Partition(topic, 0)]):
            synchronized_consumer.resume([Partition(topic, 0)])


def test_synchronized_consumer_handles_end_of_partition(
    broker: DummyBroker[KafkaPayload],
) -> None:
    topic = Topic("topic")
    commit_log_topic = Topic("commit-log")

    broker.create_topic(topic, partitions=1)
    broker.create_topic(commit_log_topic, partitions=1)

    consumer = broker.get_consumer("consumer", enable_end_of_partition=True)
    producer = broker.get_producer()
    commit_log_consumer = broker.get_consumer("commit-log-consumer")

    messages = [
        producer.produce(topic, KafkaPayload(None, f"{i}".encode("utf8"))).result(1.0)
        for i in range(2)
    ]

    synchronized_consumer: Consumer[KafkaPayload] = SynchronizedConsumer(
        consumer,
        commit_log_consumer,
        commit_log_topic=commit_log_topic,
        commit_log_groups={"leader"},
    )

    with closing(synchronized_consumer):
        synchronized_consumer.subscribe([topic])

        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit(
                        "leader", Partition(topic, 0), messages[0].get_next_offset()
                    ),
                ),
            ).result(),
        )

        assert synchronized_consumer.poll(0) == messages[0]

        # If the commit log consumer does not handle EOF, it will have crashed
        # here and will never return the next message.
        wait_for_consumer(
            commit_log_consumer,
            producer.produce(
                commit_log_topic,
                commit_codec.encode(
                    Commit(
                        "leader", Partition(topic, 0), messages[1].get_next_offset()
                    ),
                ),
            ).result(),
        )

        assert synchronized_consumer.poll(0) == messages[1]


def test_synchronized_consumer_worker_crash(broker: DummyBroker[KafkaPayload]) -> None:
    topic = Topic("topic")
    commit_log_topic = Topic("commit-log")

    broker.create_topic(topic, partitions=1)
    broker.create_topic(commit_log_topic, partitions=1)

    poll_called = Event()

    class BrokenConsumerException(Exception):
        pass

    class BrokenDummyConsumer(DummyConsumer[KafkaPayload]):
        def poll(
            self, timeout: Optional[float] = None
        ) -> Optional[Message[KafkaPayload]]:
            try:
                raise BrokenConsumerException()
            finally:
                poll_called.set()

    consumer = broker.get_consumer("consumer")
    commit_log_consumer: Consumer[KafkaPayload] = BrokenDummyConsumer(
        broker, "commit-log-consumer"
    )

    synchronized_consumer: Consumer[KafkaPayload] = SynchronizedConsumer(
        consumer,
        commit_log_consumer,
        commit_log_topic=commit_log_topic,
        commit_log_groups={"leader"},
    )

    assert poll_called.wait(1.0) is True

    # If the worker thread has exited without a close request, calling ``poll``
    # should raise an error that originated from the worker thread.

    with pytest.raises(RuntimeError) as e:
        synchronized_consumer.poll(0.0)

    assert type(e.value.__cause__) is BrokenConsumerException

    # If a close request has been sent, the normal runtime error due to the
    # closed consumer should be raised instead.

    synchronized_consumer.close()

    with pytest.raises(RuntimeError) as e:
        synchronized_consumer.poll(0.0)

    assert type(e.value.__cause__) is not BrokenConsumerException
