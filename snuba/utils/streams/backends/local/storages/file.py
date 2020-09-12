import itertools
import pickle
from datetime import datetime
from functools import cached_property
from pathlib import Path
from struct import Struct
from typing import (
    BinaryIO,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
)

from snuba.utils.codecs import Codec
from snuba.utils.streams.backends.abstract import OffsetOutOfRange
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


class FilePartition:
    def __init__(self, path: Path) -> None:
        self.__path = path

    def exists(self) -> bool:
        return self.__path.exists()

    def size(self) -> int:
        return self.__path.lstat().st_size

    @cached_property
    def reader(self) -> BinaryIO:
        return self.__path.open("rb")

    @cached_property
    def writer(self) -> BinaryIO:
        return self.__path.open("ab")


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
        self.__topic_partition_cache: MutableMapping[
            Topic, Sequence[FilePartition]
        ] = {}

    def create_topic(self, topic: Topic, partitions: int) -> None:
        topic_path = self.__directory / topic.name
        if topic_path.exists():
            raise TopicExists(topic)

        topic_path.mkdir()

        for i in range(partitions):
            (topic_path / str(i)).touch()

    def __get_file_partitions_for_topic(self, topic: Topic) -> Sequence[FilePartition]:
        if topic not in self.__topic_partition_cache:
            topic_path = self.__directory / topic.name
            if not topic_path.exists():
                raise TopicDoesNotExist(topic)

            partitions: MutableSequence[FilePartition] = []
            for i in itertools.count():
                partition = FilePartition(topic_path / str(i))
                if not partition.exists():
                    break
                else:
                    partitions.append(partition)

            self.__topic_partition_cache[topic] = partitions

        return self.__topic_partition_cache[topic]

    def __get_file_partition(self, partition: Partition) -> FilePartition:
        partitions = self.__get_file_partitions_for_topic(partition.topic)

        # TODO: Maybe this should be enforced in the ``Partition`` constructor?
        if not partition.index >= 0:
            raise PartitionDoesNotExist(partition)

        try:
            return partitions[partition.index]
        except IndexError as e:
            raise PartitionDoesNotExist(partition) from e

    def get_partition_count(self, topic: Topic) -> int:
        return len(self.__get_file_partitions_for_topic(topic))

    def produce(
        self, partition: Partition, payload: TPayload, timestamp: datetime
    ) -> Message[TPayload]:
        encoded = self.__codec.encode((payload, timestamp))
        file = self.__get_file_partition(partition).writer
        offset = file.tell()
        file.write(self.__record_header.pack(len(encoded)))
        file.write(encoded)
        file.flush()
        next_offset = file.tell()
        return Message(partition, offset, payload, timestamp, next_offset)

    def consume(self, partition: Partition, offset: int) -> Optional[Message[TPayload]]:
        file_partition = self.__get_file_partition(partition)
        file = file_partition.reader

        if file.tell() != offset:
            file.seek(offset)

        size_raw = file.read(self.__record_header.size)
        if not size_raw:
            if offset > file_partition.size():
                raise OffsetOutOfRange()
            else:
                return None

        [size] = self.__record_header.unpack(size_raw)
        payload, timestamp = self.__codec.decode(file.read(size))

        return Message(partition, offset, payload, timestamp, file.tell())
