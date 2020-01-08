from datetime import datetime
from typing import (
    Callable,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
)

from snuba.utils.streams.consumer import Consumer, ConsumerError
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


epoch = datetime(2019, 12, 19)


class DummyConsumer(Consumer[TPayload]):
    def __init__(self, messages: Mapping[Partition, Sequence[TPayload]]) -> None:
        # TODO: The message data needs to include the timestamp.
        self.__messages: Mapping[Partition, MutableSequence[TPayload]] = {
            partition: list(payloads) for partition, payloads in messages.items()
        }

        self.__subscription: Sequence[Topic] = []
        self.__assignment: Optional[Sequence[Partition]] = None

        self.__offsets: MutableMapping[Partition, int] = {}
        self.__staged_offsets: MutableMapping[Partition, int] = {}
        self.__committed_offsets: MutableMapping[Partition, int] = {}

        self.commit_offsets_calls = 0
        self.close_calls = 0

        self.__closed = False

    def extend(self, messages: Mapping[Partition, Sequence[TPayload]]) -> None:
        for partition, payloads in messages.items():
            self.__messages[partition].extend(payloads)

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        assert not self.__closed

        self.__subscription = topics

        assignment: Sequence[Partition] = [
            partition
            for partition in self.__messages
            if any(partition in topic for topic in topics)
        ]

        if self.__assignment is not None and on_revoke is not None:
            on_revoke(self.__assignment)

        self.__assignment = assignment

        # TODO: Handle offset reset more realistically.
        self.__offsets = {
            partition: self.__committed_offsets.get(partition, 0)
            for partition in assignment
        }

        self.__staged_offsets.clear()

        if on_assign is not None:
            on_assign(self.__offsets)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        assert not self.__closed

        # TODO: Throw ``EndOfPartition`` errors.
        for partition, offset in sorted(self.__offsets.items()):
            messages = self.__messages[partition]
            try:
                payload = messages[offset]
            except IndexError:
                if not offset == len(messages):
                    raise ConsumerError("invalid offset")
            else:
                message = Message(partition, offset, payload, epoch)
                self.__offsets[partition] = message.get_next_offset()
                return message

        return None

    def tell(self) -> Mapping[Partition, int]:
        return self.__offsets

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        if offsets.keys() - self.__offsets.keys():
            raise ConsumerError("cannot seek on unassigned partitions")

        self.__offsets.update(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        assert not self.__closed

        self.__staged_offsets.update(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        assert not self.__closed

        offsets = {**self.__staged_offsets}
        self.__committed_offsets.update(offsets)
        self.__staged_offsets.clear()

        self.commit_offsets_calls += 1
        return offsets

    def close(self, timeout: Optional[float] = None) -> None:
        self.__closed = True
        self.close_calls += 1
