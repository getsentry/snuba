from io import TextIOBase
from pathlib import Path
from typing import Callable, Mapping, MutableMapping, Optional, Sequence

from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


class FileConsumer(Consumer[TPayload]):
    def __init__(self, directory: str) -> None:
        self.__directory = Path(directory)
        assert self.__directory.is_dir()

        self.__assignment: MutableMapping[Partition, TextIOBase] = {}

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        # TODO: Trigger callbacks.

        self.__assignment.clear()

        for topic in topics:
            path = self.__directory.joinpath(topic.name)
            if not path.is_dir():
                continue

            for f in path.iterdir():
                self.__assignment[Partition(topic, int(f.name))] = f.open("r")

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        for partition, file in self.__assignment.items():
            offset = file.tell()
            line = file.readline().strip()
            if line:
                # TODO: Include timestamp.
                return Message(partition, offset, line, None)

        return None

    def tell(self) -> Mapping[Partition, int]:
        return {partition: file.tell() for partition, file in self.__assignment.items()}

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        raise NotImplementedError  # TODO

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        raise NotImplementedError  # TODO

    def commit_offsets(self) -> Mapping[Partition, int]:
        raise NotImplementedError  # TODO

    def close(self, timeout: Optional[float] = None) -> None:
        raise NotImplementedError
