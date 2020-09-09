import itertools
import pickle
from datetime import datetime
from pathlib import Path
from struct import Struct
from typing import BinaryIO, MutableMapping, Optional, Tuple

from snuba.utils.clock import Clock, SystemClock
from snuba.utils.codecs import Codec
from snuba.utils.streams.backends.local.storages.abstract import MessageStorage
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


class PickleCodec(Codec[bytes, Tuple[TPayload, datetime]]):
    def encode(self, value: Tuple[TPayload, datetime]) -> bytes:
        return pickle.dumps(value)

    def decode(self, value: bytes) -> Tuple[TPayload, datetime]:
        return pickle.loads(value)


class FileMessageStorage(MessageStorage[TPayload]):
    def __init__(
        self,
        directory: str,
        codec: Codec[bytes, Tuple[TPayload, datetime]] = PickleCodec(),
        clock: Clock = SystemClock(),
    ) -> None:
        self.__directory = Path(directory)
        assert self.__directory.exists() and self.__directory.is_dir()
        self.__codec = codec
        self.__clock = clock

        self.__record_header = Struct("!L")
        self.__readers: MutableMapping[Partition, BinaryIO] = {}
        self.__writers: MutableMapping[Partition, BinaryIO] = {}

    def create_topic(self, topic: Topic, partitions: int) -> None:
        topic_path = self.__directory / topic.name
        if topic_path.exists():
            raise ValueError("topic already exists")

        topic_path.mkdir()

        for i in range(partitions):
            (topic_path / str(i)).touch()

    def get_partition_count(self, topic: Topic) -> int:
        topic_path = self.__directory / topic.name
        if not topic_path.exists():
            raise KeyError("topic does not exist")

        for i in itertools.count():
            if not (topic_path / str(i)).exists():
                return i

        raise Exception  # unreachable

    def produce(self, partition: Partition, payload: TPayload) -> Message[TPayload]:
        timestamp = datetime.fromtimestamp(self.__clock.time())
        encoded = self.__codec.encode((payload, timestamp))
        try:
            file = self.__writers[partition]
        except KeyError:
            file = self.__writers[partition] = (
                self.__directory / partition.topic.name / str(partition.index)
            ).open("ab")
        offset = file.tell()
        file.write(self.__record_header.pack(len(encoded)))
        file.write(encoded)
        file.flush()
        next_offset = file.tell()
        return Message(partition, offset, payload, timestamp, next_offset)

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        try:
            file = self.__readers[partition]
        except KeyError:
            file = self.__readers[partition] = (
                self.__directory / partition.topic.name / str(partition.index)
            ).open("rb")

        if file.tell() != offset:
            file.seek(offset)

        size_raw = file.read(self.__record_header.size)
        if not size_raw:
            return None  # XXX: cannot distinguish between end and invalid

        [size] = self.__record_header.unpack(size_raw)
        payload, timestamp = self.__codec.decode(file.read(size))

        return Message(partition, offset, payload, timestamp, file.tell())
