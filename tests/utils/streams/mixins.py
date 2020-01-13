import uuid
from abc import ABC, abstractmethod
from concurrent.futures import wait
from contextlib import closing
from typing import ContextManager, Mapping, Sequence

import pytest

from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.producer import Producer
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
                assert (
                    wait(
                        [producer.produce(topic, i) for i in range(2)], timeout=5.0,
                    ).not_done
                    == set()
                )

            consumer = self.get_consumer(group)

            def assignment_callback(partitions: Mapping[Partition, int]) -> None:
                assignment_callback.called = True
                assert partitions == {Partition(topic, 0): 0}

                consumer.seek({Partition(topic, 0): 1})

                with pytest.raises(ConsumerError):
                    consumer.seek({Partition(topic, 1): 0})

            def revocation_callback(partitions: Sequence[Partition]) -> None:
                revocation_callback.called = True
                assert partitions == [Partition(topic, 0)]
                assert consumer.tell() == {Partition(topic, 0): 1}

                # Not sure why you'd want to do this, but it shouldn't error.
                consumer.seek({Partition(topic, 0): 0})

            # TODO: It'd be much nicer if ``subscribe`` returned a future that we could
            # use to wait for assignment, but we'd need to be very careful to avoid
            # edge cases here. It's probably not worth the complexity for now.
            consumer.subscribe(
                [topic], on_assign=assignment_callback, on_revoke=revocation_callback
            )

            message = consumer.poll(10.0)  # XXX: getting the subcription is slow
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == 1
            assert message.payload == 1

            assert consumer.tell() == {Partition(topic, 0): 2}
            assert getattr(assignment_callback, "called", False)

            consumer.seek({Partition(topic, 0): 0})
            assert consumer.tell() == {Partition(topic, 0): 0}

            with pytest.raises(ConsumerError):
                consumer.seek({Partition(topic, 1): 0})

            consumer.pause([Partition(topic, 0)])

            consumer.resume([Partition(topic, 0)])

            message = consumer.poll(1.0)
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == 0
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
                consumer.seek({Partition(topic, 0): 0})

            consumer.close()

            with pytest.raises(RuntimeError):
                consumer.subscribe([topic])

            with pytest.raises(RuntimeError):
                consumer.unsubscribe()

            with pytest.raises(RuntimeError):
                consumer.poll()

            with pytest.raises(RuntimeError):
                consumer.tell()

            with pytest.raises(RuntimeError):
                consumer.seek({Partition(topic, 0): 0})

            with pytest.raises(RuntimeError):
                consumer.pause([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.resume([Partition(topic, 0)])

            with pytest.raises(RuntimeError):
                consumer.stage_offsets({})

            with pytest.raises(RuntimeError):
                consumer.commit_offsets()

            consumer.close()

            consumer = self.get_consumer(group)

            consumer.subscribe([topic])

            message = consumer.poll(10.0)  # XXX: getting the subscription is slow
            assert isinstance(message, Message)
            assert message.partition == Partition(topic, 0)
            assert message.offset == 1
            assert message.payload == 1

            try:
                assert consumer.poll(1.0) is None
            except EndOfPartition as error:
                assert error.partition == Partition(topic, 0)
                assert error.offset == 2
            else:
                raise AssertionError("expected EndOfPartition error")

            consumer.close()
