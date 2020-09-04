from __future__ import annotations

import functools
import itertools
import pathlib
import random
import struct
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from io import BufferedWriter, BufferedReader
from typing import Callable, Iterable, Mapping, Optional, Sequence, Tuple, Union

from bidict import bidict

from snuba.utils.codecs import Decoder, Encoder
from snuba.utils.streams.consumer import Consumer
from snuba.utils.streams.producer import Producer
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


@dataclass(frozen=True)
class Assignment:
    topics: Sequence[Topic]
    on_assign: Optional[Callable[[Mapping[Partition, int]], None]]
    on_revoke: Optional[Callable[[Sequence[Partition]], None]]


header = struct.Struct("!L")


class MultipleFileReader:
    def __init__(self, files: Iterable[BufferedReader]):
        self.__files = set(files)
        self.__paused = set()

    def read(self) -> Optional[Tuple[BufferedReader, bytes]]:
        for file in self.__files - self.__paused:
            value = file.read(header.size)
            if value:
                [size] = header.unpack(value)
                return file, file.read(size)

        return None

    def pause(self, file: BufferedReader) -> None:
        assert file in self.__files
        self.__paused.add(file)

    def resume(self, file: BufferedReader) -> None:
        assert file in self.__files
        self.__paused.discard(file)

    def close(self) -> None:
        for f in self.__files:
            f.close()


class FileConsumer(Consumer[TPayload]):
    def __init__(
        self, directory: str, decoder: Decoder[bytes, Tuple[TPayload, datetime]]
    ):
        self.__directory = pathlib.Path(directory)
        assert self.__directory.is_dir()

        self.__decoder = decoder

        self.__assignment: Optional[Assignment] = None

        self.__reader: Optional[MultipleFileReader] = None
        self.__partition_file_map: Optional[bidict[Partition, BufferedReader]] = None

        self.__callbacks = []

    def __create_assignment(self, assignment: Assignment) -> None:
        # If an assignment already exists, we need to revoke it before
        # establishing this assignment.
        if self.__assignment is not None:
            self.__delete_assignment()

        self.__assignment = assignment

        self.__partition_file_map = bidict()

        for topic in self.__assignment.topics:
            topic_path = self.__directory.joinpath(topic.name)
            if topic_path.exists() and topic_path.is_dir():
                for partition_index in itertools.count():
                    partition_path = topic_path.joinpath(f"{partition_index}")
                    if partition_path.exists() and partition_path.is_file():
                        self.__partition_file_map[
                            Partition(topic, partition_index)
                        ] = open(partition_path, "rb")
                    else:
                        break

        self.__reader = MultipleFileReader(self.__partition_file_map.values())

        if self.__assignment.on_assign is not None:
            self.__assignment.on_assign(
                {
                    partition: file.tell()
                    for partition, file in self.__partition_file_map.items()
                }
            )

    def __delete_assignment(self) -> None:
        if self.__assignment is None:
            return  # nothing to do

        if self.__assignment.on_revoke is not None:
            self.__assignment.on_revoke([*self.__partition_file_map.keys()])

        assert self.__reader is not None
        self.__reader.close()
        self.__reader = None

        self.__partition_file_map = None

        self.__assignment = None

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        self.__callbacks.append(
            functools.partial(
                self.__create_assignment, Assignment(topics, on_assign, on_revoke)
            )
        )

    def unsubscribe(self) -> None:
        self.__callbacks.append(self.__delete_assignment)

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        while self.__callbacks:
            callback = self.__callbacks.pop()
            callback()

        if not self.__partition_file_map:
            return None

        # TODO: Support end-of-file?
        assert self.__reader is not None
        value = self.__reader.read()
        if value is None:
            return None

        file, encoded = value
        payload, timestamp = self.__decoder.decode(encoded)

        # TODO: Correctly report `get_next_offset` based on line length.
        return Message(
            self.__partition_file_map.inverse[file],
            file.tell() - (len(encoded) + header.size),
            payload,
            timestamp,
        )

    def pause(self, partitions: Sequence[Partition]) -> None:
        assert self.__partition_file_map is not None and self.__reader is not None
        assert not set(partitions) - self.__partition_file_map.keys()

        for partition in partitions:
            self.__reader.pause(self.__partition_file_map[partition])

    def resume(self, partitions: Sequence[Partition]) -> None:
        assert self.__partition_file_map is not None and self.__reader is not None
        assert not set(partitions) - self.__partition_file_map.keys()

        for partition in partitions:
            self.__reader.resume(self.__partition_file_map[partition])

    def paused(self) -> Sequence[Partition]:
        raise NotImplementedError

    def tell(self) -> Mapping[Partition, int]:
        assert self.__partition_file_map is not None
        return {
            partition: file.tell()
            for partition, file in self.__partition_file_map.items()
        }

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        raise NotImplementedError

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        raise NotImplementedError

    def commit_offsets(self) -> Mapping[Partition, int]:
        raise NotImplementedError

    def close(self, timeout: Optional[float] = None) -> None:
        raise NotImplementedError

    def closed(self) -> bool:
        raise NotImplementedError


class FileProducer(Producer[TPayload]):
    def __init__(
        self, directory: str, encoder: Encoder[bytes, Tuple[TPayload, datetime]]
    ):
        self.__directory = pathlib.Path(directory)
        assert self.__directory.is_dir()

        self.__encoder = encoder

        self.__file_cache: Mapping[Topic, Sequence[BufferedWriter]] = defaultdict(list)

    def __get_topic_files(self, topic: Topic) -> Sequence[BufferedWriter]:
        if topic in self.__file_cache:
            return self.__file_cache[topic]

        files = []
        for i in itertools.count():
            path = self.__directory / topic.name / str(i)
            if not path.exists():
                break

            files.append(open(path, "ab"))

        self.__file_cache[topic] = files

        return files

    def __get_file(self, partition: Partition) -> BufferedWriter:
        return self.__get_topic_files(partition.topic)[partition.index]

    def produce(
        self, destination: Union[Topic, Partition], payload: TPayload
    ) -> Future[Message[TPayload]]:
        if isinstance(destination, Topic):
            files = self.__get_topic_files(destination)
            index = random.randint(0, len(files) - 1)
            partition = Partition(destination, index)
            file = files[index]
        elif isinstance(destination, Partition):
            partition = destination
            file = self.__get_topic_files(partition.topic)[partition.index]
        else:
            raise TypeError

        offset = file.tell()
        timestamp = datetime.now()
        encoded = self.__encoder.encode((payload, timestamp))
        file.write(header.pack(len(encoded)))
        file.write(encoded)
        result = Future()
        result.set_result(Message(partition, offset, payload, timestamp))
        return result

    def close(self) -> Future[None]:
        for files in self.__file_cache.values():
            for file in files:
                file.close()

        result = Future()
        result.set_result(None)
        return result


import pickle
from snuba.utils.codecs import Codec
from snuba.utils.streams.kafka import KafkaPayload


class KafkaPayloadFileCodec(Codec[bytes, Tuple[KafkaPayload, datetime]]):
    def encode(self, value: Tuple[KafkaPayload, datetime]) -> bytes:
        return pickle.dumps(value)

    def decode(self, value: bytes) -> Tuple[KafkaPayload, datetime]:
        return pickle.loads(value)


kafka_payload_file_codec = KafkaPayloadFileCodec()


if __name__ == "__main__":
    import sys

    topics = [Topic(t) for t in sys.argv[2:]]

    p = FileProducer(sys.argv[1], kafka_payload_file_codec)

    c = FileConsumer(sys.argv[1], kafka_payload_file_codec)
    c.subscribe(topics)
