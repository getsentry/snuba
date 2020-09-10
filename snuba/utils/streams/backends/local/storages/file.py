import itertools
import pickle
from datetime import datetime
from pathlib import Path
from struct import Struct
from typing import BinaryIO, MutableMapping, MutableSequence, Optional, Sequence, Tuple

from snuba.utils.codecs import Codec
from snuba.utils.streams.backends.local.storages.abstract import (
    MessageStorage,
    PartitionDoesNotExist,
    TopicDoesNotExist,
    TopicExists,
)
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
    ) -> None:
        self.__directory = Path(directory)
        assert self.__directory.exists() and self.__directory.is_dir()
        self.__codec = codec

        self.__record_header = Struct("!L")
        self.__readers: MutableMapping[Topic, Sequence[BinaryIO]] = {}
        self.__writers: MutableMapping[Topic, Sequence[BinaryIO]] = {}

    def create_topic(self, topic: Topic, partitions: int) -> None:
        topic_path = self.__directory / topic.name
        if topic_path.exists():
            raise TopicExists(topic)

        topic_path.mkdir()

        for i in range(partitions):
            (topic_path / str(i)).touch()

    def __get_partitions(self, topic: Topic) -> Sequence[Path]:
        topic_path = self.__directory / topic.name
        if not topic_path.exists():
            raise TopicDoesNotExist(topic)

        paths: MutableSequence[Path] = []
        for i in itertools.count():
            partition_path = topic_path / str(i)
            if not partition_path.exists():
                return paths
            else:
                paths.append(partition_path)

        raise Exception  # unreachable

    def get_partition_count(self, topic: Topic) -> int:
        return len(self.__get_partitions(topic))

    def __get_reader(self, partition: Partition) -> BinaryIO:
        # TODO: Maybe this should be enforced in the ``Partition`` constructor?
        if not partition.index >= 0:
            raise PartitionDoesNotExist(partition)

        try:
            partitions = self.__readers[partition.topic]
        except KeyError:
            partitions = self.__readers[partition.topic] = [
                path.open("rb") for path in self.__get_partitions(partition.topic)
            ]

        try:
            return partitions[partition.index]
        except IndexError as e:
            raise PartitionDoesNotExist(partition) from e

    def __get_writer(self, partition: Partition) -> BinaryIO:
        # TODO: Maybe this should be enforced in the ``Partition`` constructor?
        if not partition.index >= 0:
            raise PartitionDoesNotExist(partition)

        try:
            partitions = self.__writers[partition.topic]
        except KeyError:
            partitions = self.__writers[partition.topic] = [
                path.open("ab") for path in self.__get_partitions(partition.topic)
            ]

        try:
            return partitions[partition.index]
        except IndexError as e:
            raise PartitionDoesNotExist(partition) from e

    def produce(
        self, partition: Partition, payload: TPayload, timestamp: datetime
    ) -> Message[TPayload]:
        encoded = self.__codec.encode((payload, timestamp))
        file = self.__get_writer(partition)
        offset = file.tell()
        file.write(self.__record_header.pack(len(encoded)))
        file.write(encoded)
        file.flush()
        next_offset = file.tell()
        return Message(partition, offset, payload, timestamp, next_offset)

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        file = self.__get_reader(partition)

        if file.tell() != offset:
            file.seek(offset)

        size_raw = file.read(self.__record_header.size)
        if not size_raw:
            return None  # XXX: cannot distinguish between end and invalid

        [size] = self.__record_header.unpack(size_raw)
        payload, timestamp = self.__codec.decode(file.read(size))

        return Message(partition, offset, payload, timestamp, file.tell())
