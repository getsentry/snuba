import time
import uuid
from abc import ABC, abstractmethod
from contextlib import closing
from typing import ContextManager, Generic, Iterator, Mapping, Optional, Sequence
from unittest import mock

import pytest

from snuba.utils.streams.backends.abstract import (
    Consumer,
    ConsumerError,
    EndOfPartition,
    Producer,
)
from snuba.utils.streams.types import Message, Partition, Topic, TPayload
from tests.assertions import assert_changes, assert_does_not_change


class StreamsTestMixin(ABC, Generic[TPayload]):
    @abstractmethod
    def get_topic(self, partitions: int = 1) -> ContextManager[Topic]:
        raise NotImplementedError

    @abstractmethod
    def get_consumer(
        self, group: Optional[str] = None, enable_end_of_partition: bool = True
    ) -> Consumer[TPayload]:
        raise NotImplementedError

    @abstractmethod
    def get_producer(self) -> Producer[TPayload]:
        raise NotImplementedError

    @abstractmethod
    def get_payloads(self) -> Iterator[TPayload]:
        raise NotImplementedError

    def test_consumer(self) -> None:
        group = uuid.uuid1().hex
        payloads = self.get_payloads()

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages = [
                    future.result(timeout=5.0)
                    for future in [
                        producer.produce(topic, next(payloads)) for i in range(2)
                    ]
                ]

            consumer = self.get_consumer(group)

            def assignment_callback(partitions: Mapping[Partition, int]) -> None:
                assignment_callback.called = True
                assert partitions == {Partition(topic, 0): messages[0].offset}

                consumer.seek({Partition(topic, 0): messages[1].offset})

                with pytest.raises(ConsumerError):
                    consumer.seek({Partition(topic, 1): 0})

            assignment_callback.called = False

            def revocation_callback(partitions: Sequence[Partition]) -> None:
                revocation_callback.called = True
                assert partitions == [Partition(topic, 0)]
                assert consumer.tell() == {Partition(topic, 0): messages[1].offset}

                # Not sure why you'd want to do this, but it shouldn't error.
                consumer.seek({Partition(topic, 0): messages[0].offset})

            revocation_callback.called = False

            # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
            # use to wait for assignment, but we'd need to be very careful to avoid
            # edge cases here. It's probably not worth the complexity for now.
            consumer.subscribe(
                [topic], on_assign=assignment_callback, on_revoke=revocation_callback
            )

            with assert_changes(
                lambda: assignment_callback.called, False, True
            ), assert_changes(
                consumer.tell, {}, {Partition(topic, 0): messages[1].get_next_offset()}
            ):
                message = consumer.poll(10.0)  # XXX: getting the subcription is slow

            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[1].offset
            assert message.payload == messages[1].payload

            consumer.seek({Partition(topic, 0): messages[0].offset})
            assert consumer.tell() == {Partition(topic, 0): messages[0].offset}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 1): 0})

            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            # Even if there is another message available, ``poll`` should
            # return ``None`` if the consumer is paused.
            assert consumer.poll(1.0) is None

            with assert_changes(consumer.paused, [Partition(topic, 0)], []):
                consumer.resume([Partition(topic, 0)])

            message = consumer.poll(1.0)
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[0].offset
            assert message.payload == messages[0].payload

            assert consumer.commit_offsets() == {}

            consumer.stage_offsets({message.partition: message.get_next_offset()})

            with pytest.raises(ConsumerError):
                consumer.stage_offsets({Partition(Topic("invalid"), 0): 0})

            assert consumer.commit_offsets() == {
                Partition(topic, 0): message.get_next_offset()
            }

            assert consumer.tell() == {Partition(topic, 0): messages[1].offset}

            consumer.unsubscribe()

            with assert_changes(lambda: revocation_callback.called, False, True):
                assert consumer.poll(1.0) is None

            assert consumer.tell() == {}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 0): messages[0].offset})

            revocation_callback.called = False

            with assert_changes(
                lambda: consumer.closed, False, True
            ), assert_does_not_change(lambda: revocation_callback.called, False):
                consumer.close()

            # Make sure all public methods (except ``close```) error if called
            # after the consumer has been closed.

            with pytest.raises(RuntimeError):
                consumer.subscribe([topic])

            with pytest.raises(RuntimeError):
                consumer.unsubscribe()

            with pytest.raises(RuntimeError):
                consumer.poll()

            with pytest.raises(RuntimeError):
                consumer.tell()

            with pytest.raises(RuntimeError):
                consumer.seek({Partition(topic, 0): messages[0].offset})

            with pytest.raises(RuntimeError):
                consumer.pause([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.resume([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.paused()

            with pytest.raises(RuntimeError):
                consumer.stage_offsets({})

            with pytest.raises(RuntimeError):
                consumer.commit_offsets()

            consumer.close()  # should be safe, even if the consumer is already closed

            consumer = self.get_consumer(group)

            revocation_callback = mock.MagicMock()

            consumer.subscribe([topic], on_revoke=revocation_callback)

            message = consumer.poll(10.0)  # XXX: getting the subscription is slow
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[1].offset
            assert message.payload == messages[1].payload

            try:
                assert consumer.poll(1.0) is None
            except EndOfPartition as error:
                assert error.partition == Partition(topic, 0)
                assert error.offset == message.get_next_offset()
            else:
                raise AssertionError("expected EndOfPartition error")

            with assert_changes(lambda: revocation_callback.called, False, True):
                consumer.close()

    def test_working_offsets(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages = [producer.produce(topic, next(payloads)).result(5.0)]

            def on_assign(partitions: Mapping[Partition, int]) -> None:
                # NOTE: This will eventually need to be controlled by a generalized
                # consumer auto offset reset setting.
                assert (
                    partitions
                    == consumer.tell()
                    == {messages[0].partition: messages[0].offset}
                )

            consumer = self.get_consumer()
            consumer.subscribe([topic], on_assign=on_assign)

            for i in range(5):
                message = consumer.poll(1.0)
                if message is not None:
                    break
                else:
                    time.sleep(1.0)
            else:
                raise Exception("assignment never received")

            assert message == messages[0]

            # The first call to ``poll`` should raise ``EndOfPartition``. It
            # should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            with assert_does_not_change(
                consumer.tell, {message.partition: message.get_next_offset()}
            ), pytest.raises(EndOfPartition):
                consumer.poll(1.0) is None

            # It should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            with assert_does_not_change(
                consumer.tell, {message.partition: message.get_next_offset()}
            ):
                assert consumer.poll(1.0) is None

            with assert_changes(
                consumer.tell,
                {message.partition: message.get_next_offset()},
                {message.partition: message.offset},
            ):
                consumer.seek({message.partition: message.offset})

            with assert_changes(
                consumer.tell,
                {message.partition: message.offset},
                {message.partition: message.get_next_offset()},
            ):
                assert consumer.poll(1.0) == messages[0]

            # Seeking beyond the first missing index should work but subsequent
            # reads should error. (We don't know if this offset is valid or not
            # until we try to fetch a message.)
            with assert_changes(
                consumer.tell,
                {message.partition: message.get_next_offset()},
                {message.partition: message.get_next_offset() + 1},
            ):
                consumer.seek({message.partition: message.get_next_offset() + 1})

            # Offsets should not be advanced after a failed poll.
            with assert_does_not_change(
                consumer.tell, {message.partition: message.get_next_offset() + 1}
            ), pytest.raises(ConsumerError):
                consumer.poll(1.0)

            # Trying to seek on an unassigned partition should error.
            with assert_does_not_change(
                consumer.tell, {message.partition: message.get_next_offset() + 1}
            ), pytest.raises(ConsumerError):
                consumer.seek({message.partition: 0, Partition(topic, -1): 0})

            # Trying to seek to a negative offset should error.
            with assert_does_not_change(
                consumer.tell, {message.partition: message.get_next_offset() + 1}
            ), pytest.raises(ConsumerError):
                consumer.seek({message.partition: -1})

    def test_pause_resume(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic() as topic, closing(
            self.get_consumer()
        ) as consumer, closing(self.get_producer()) as producer:
            messages = [
                producer.produce(topic, next(payloads)).result(timeout=5.0)
                for i in range(5)
            ]

            consumer.subscribe([topic])

            assert consumer.poll(10.0) == messages[0]
            assert consumer.paused() == []

            # XXX: Unfortunately, there is really no way to prove that this
            # consumer would return the message other than by waiting a while.
            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            assert consumer.poll(1.0) is None

            # We should pick up where we left off when we resume the partition.
            with assert_changes(consumer.paused, [Partition(topic, 0)], []):
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[1]

            # Calling ``seek`` should have a side effect, even if no messages
            # are consumed before calling ``pause``.
            with assert_changes(
                consumer.tell,
                {Partition(topic, 0): messages[1].get_next_offset()},
                {Partition(topic, 0): messages[3].offset},
            ):
                consumer.seek({Partition(topic, 0): messages[3].offset})
                consumer.pause([Partition(topic, 0)])
                assert consumer.poll(1.0) is None
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[3]

            # It is still allowable to call ``seek`` on a paused partition.
            # When consumption resumes, we would expect to see the side effect
            # of that seek.
            consumer.pause([Partition(topic, 0)])
            with assert_changes(
                consumer.tell,
                {Partition(topic, 0): messages[3].get_next_offset()},
                {Partition(topic, 0): messages[0].offset},
            ):
                consumer.seek({Partition(topic, 0): messages[0].offset})
                assert consumer.poll(1.0) is None
                consumer.resume([Partition(topic, 0)])

            assert consumer.poll(5.0) == messages[0]

            with assert_does_not_change(consumer.paused, []), pytest.raises(
                ConsumerError
            ):
                consumer.pause([Partition(topic, 0), Partition(topic, 1)])

            with assert_changes(consumer.paused, [], [Partition(topic, 0)]):
                consumer.pause([Partition(topic, 0)])

            with assert_does_not_change(
                consumer.paused, [Partition(topic, 0)]
            ), pytest.raises(ConsumerError):
                consumer.resume([Partition(topic, 0), Partition(topic, 1)])

    def test_pause_resume_rebalancing(self) -> None:
        payloads = self.get_payloads()

        with self.get_topic(2) as topic, closing(
            self.get_producer()
        ) as producer, closing(
            self.get_consumer("group", enable_end_of_partition=False)
        ) as consumer_a, closing(
            self.get_consumer("group", enable_end_of_partition=False)
        ) as consumer_b:
            messages = [
                producer.produce(Partition(topic, i), next(payloads)).result(
                    timeout=5.0
                )
                for i in range(2)
            ]

            consumer_a.subscribe([topic])

            # It doesn't really matter which message is fetched first -- we
            # just want to know the assignment occurred.
            assert (
                consumer_a.poll(10.0) in messages
            )  # XXX: getting the subcription is slow

            assert len(consumer_a.tell()) == 2
            assert len(consumer_b.tell()) == 0

            # Pause all partitions.
            consumer_a.pause([Partition(topic, 0), Partition(topic, 1)])
            assert set(consumer_a.paused()) == set(
                [Partition(topic, 0), Partition(topic, 1)]
            )

            consumer_b.subscribe([topic])
            for i in range(10):
                assert consumer_a.poll(0) is None  # attempt to force session timeout
                if consumer_b.poll(1.0) is not None:
                    break
            else:
                assert False, "rebalance did not occur"

            # The first consumer should have had its offsets rolled back, as
            # well as should have had it's partition resumed during
            # rebalancing.
            assert consumer_a.paused() == []
            assert consumer_a.poll(10.0) is not None

            assert len(consumer_a.tell()) == 1
            assert len(consumer_b.tell()) == 1
