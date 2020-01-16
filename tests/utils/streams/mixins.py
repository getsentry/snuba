import time
import uuid
from abc import ABC, abstractmethod
from contextlib import closing
from typing import ContextManager, Mapping, Optional, Sequence

import pytest

from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.producer import MessageDetails, Producer
from snuba.utils.streams.types import Message, Partition, Topic


class StreamsTestMixin(ABC):
    @abstractmethod
    def get_topic(self) -> ContextManager[Topic]:
        raise NotImplementedError

    @abstractmethod
    def get_consumer(self, group: str) -> Consumer[int]:
        raise NotImplementedError

    @abstractmethod
    def get_producer(self) -> Producer[int]:
        raise NotImplementedError

    def test_consumer(self) -> None:
        group = uuid.uuid1().hex

        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                messages: Sequence[MessageDetails] = [
                    future.result(timeout=5.0)
                    for future in [producer.produce(topic, i) for i in range(2)]
                ]

            consumer = self.get_consumer(group)

            def assignment_callback(partitions: Mapping[Partition, int]) -> None:
                assignment_callback.called = True
                assert partitions == {Partition(topic, 0): messages[0].offset}

                consumer.seek({Partition(topic, 0): messages[1].offset})

                with pytest.raises(ConsumerError):
                    consumer.seek({Partition(topic, 1): 0})

            def revocation_callback(partitions: Sequence[Partition]) -> None:
                revocation_callback.called = True
                assert partitions == [Partition(topic, 0)]
                assert consumer.tell() == {Partition(topic, 0): messages[1].offset}

                # Not sure why you'd want to do this, but it shouldn't error.
                consumer.seek({Partition(topic, 0): messages[0].offset})

            # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
            # use to wait for assignment, but we'd need to be very careful to avoid
            # edge cases here. It's probably not worth the complexity for now.
            consumer.subscribe(
                [topic], on_assign=assignment_callback, on_revoke=revocation_callback
            )

            message = consumer.poll(10.0)  # XXX: getting the subcription is slow
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[1].offset
            assert message.payload == 1

            assert consumer.tell() == {Partition(topic, 0): message.get_next_offset()}
            assert getattr(assignment_callback, "called", False)

            consumer.seek({Partition(topic, 0): messages[0].offset})
            assert consumer.tell() == {Partition(topic, 0): messages[0].offset}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 1): 0})

            # consumer.pause([Partition(topic, 0)])

            # consumer.resume([Partition(topic, 0)])

            message = consumer.poll(1.0)
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[0].offset
            assert message.payload == 0

            assert consumer.commit_offsets() == {}

            consumer.stage_offsets({message.partition: message.get_next_offset()})

            with pytest.raises(ConsumerError):
                consumer.stage_offsets({Partition(Topic("invalid"), 0): 0})

            assert consumer.commit_offsets() == {
                Partition(topic, 0): message.get_next_offset()
            }

            consumer.unsubscribe()

            assert consumer.poll(1.0) is None

            assert consumer.tell() == {}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 0): messages[0].offset})

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

            # with pytest.raises(RuntimeError):
            #     consumer.pause([Partition(topic, 0)])

            # with pytest.raises(RuntimeError):
            #     consumer.resume([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.stage_offsets({})

            with pytest.raises(RuntimeError):
                consumer.commit_offsets()

            consumer.close()  # should be safe, even if the consumer is already closed

            consumer = self.get_consumer(group)

            consumer.subscribe([topic])

            message = consumer.poll(10.0)  # XXX: getting the subscription is slow
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == messages[1].offset
            assert message.payload == 1

            try:
                assert consumer.poll(1.0) is None
            except EndOfPartition as error:
                assert error.partition == Partition(topic, 0)
                assert error.offset == message.get_next_offset()
            else:
                raise AssertionError("expected EndOfPartition error")

            consumer.close()

    def test_working_offsets(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                (partition, offset) = producer.produce(topic, 0).result(timeout=5.0)

            def on_assign(partitions: Mapping[Partition, int]) -> None:
                # NOTE: This will eventually need to be controlled by a generalized
                # consumer auto offset reset setting.
                assert partitions == consumer.tell() == {partition: offset}

            consumer = self.get_consumer("group")
            consumer.subscribe([topic], on_assign=on_assign)

            for i in range(5):
                message: Optional[Message[int]] = consumer.poll(1.0)
                if message is not None:
                    break
                else:
                    time.sleep(1.0)
            else:
                raise Exception("assignment never received")

            assert isinstance(message, Message)
            assert message.offset == offset
            assert consumer.tell() == {partition: message.get_next_offset()}

            # The first call to ``poll`` should raise ``EndOfPartition``. It
            # should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            with pytest.raises(EndOfPartition):
                consumer.poll(1.0) is None

            # It should be otherwise be safe to try to read the first missing
            # offset (index) in the partition.
            assert consumer.poll(1.0) is None

            consumer.seek({partition: offset})
            assert consumer.tell() == {partition: offset}

            message = consumer.poll(1.0)
            assert isinstance(message, Message)
            assert message.offset == offset
            assert consumer.tell() == {partition: message.get_next_offset()}

            # Seeking beyond the first missing index should work, but subsequent reads
            # should error.
            consumer.seek({partition: message.get_next_offset() + 1})
            assert consumer.tell() == {partition: message.get_next_offset() + 1}

            with pytest.raises(ConsumerError):
                consumer.poll(1.0)

            # Offsets should not be advanced after a failed poll.
            assert consumer.tell() == {partition: message.get_next_offset() + 1}

            # Trying to seek on an unassigned partition should error.
            with pytest.raises(ConsumerError):
                consumer.seek({partition: 0, Partition(topic, -1): 0})

            # ``seek`` should be atomic -- either all updates are applied or none are.
            assert consumer.tell() == {partition: message.get_next_offset() + 1}

            # Trying to seek to a negative offset should error.
            with pytest.raises(ConsumerError):
                consumer.seek({partition: -1})

            assert consumer.tell() == {partition: message.get_next_offset() + 1}
