from dataclasses import dataclass
from datetime import datetime
from random import Random
from typing import Callable, Mapping, MutableMapping, Optional, Sequence

from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


@dataclass
class PartitionState:
    paused: bool = False
    offset: int = 0
    staged_offset: Optional[int] = None


class RandomDataGeneratorConsumer(Consumer[TPayload]):
    def __init__(
        self,
        topics: Mapping[Topic, int],
        generator: Callable[[Random], TPayload],
        random: Optional[Random] = None,
    ):
        if random is None:
            random = Random()

        self.__topics = topics
        self.__generator = generator
        self.__random = random
        self.__state: MutableMapping[Partition, PartitionState] = {}
        self.__closed = False

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        for topic, partitions in self.__topics.items():
            for index in range(partitions):
                self.__state[Partition(topic, index)] = PartitionState()

        if on_assign is not None:
            on_assign(
                {partition: state.offset for partition, state in self.__state.items()}
            )

    def unsubscribe(self) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        self.__state.clear()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        # TODO: Enable throwing `EndOfPartition` with some probability.
        # TODO: Enable delaying the message with `time.sleep` with some probability.
        try:
            partition, state = self.__random.choice([*self.__state.items()])
        except IndexError:
            return None

        offset = state.offset
        state.offset = offset + 1
        message = Message(
            partition, offset, self.__generator(self.__random), datetime.utcnow()
        )
        return message

    def pause(self, partitions: Sequence[Partition]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.__state.keys():
            raise ConsumerError("cannot pause unassigned partitions")

        for partition in partitions:
            self.__state[partition].paused = True

    def resume(self, partitions: Sequence[Partition]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.__state.keys():
            raise ConsumerError("cannot resume unassigned partitions")

        for partition in partitions:
            self.__state[partition].paused = False

    def paused(self) -> Sequence[Partition]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        return [
            partition for partition, state in self.__state.items() if not state.paused
        ]

    def tell(self) -> Mapping[Partition, int]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        return {partition: state.offset for partition, state in self.__state.items()}

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        if offsets.keys() - self.__state.keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        for partition, offset in offsets.items():
            self.__state[partition].offset = offset

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        assert not offsets.keys() - self.__state.keys()

        for partition, offset in offsets.items():
            self.__state[partition].staged_offset = offset

    def commit_offsets(self) -> Mapping[Partition, int]:
        if self.__closed:
            raise RuntimeError("consumer is closed")

        offsets = {}
        for partition, state in self.__state.items():
            if state.staged_offset is not None:
                offsets[partition] = state.staged_offset
                state.staged_offset = None
        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        self.__closed = True
