import contextlib
import itertools
import os
import pickle
import uuid
from contextlib import closing
from pickle import PickleBuffer
from typing import Iterator, MutableSequence, Optional
from unittest import TestCase

import pytest
from arroyo.backends.kafka import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.kafka.configuration import build_kafka_configuration
from arroyo.backends.kafka.consumer import as_kafka_configuration_bool
from arroyo.errors import ConsumerError, EndOfPartition
from arroyo.synchronized import Commit, commit_codec
from arroyo.tests.backends.mixins import StreamsTestMixin
from arroyo.types import Message, Partition, Topic
from confluent_kafka.admin import AdminClient, NewTopic


def test_payload_equality() -> None:
    assert KafkaPayload(None, b"", []) == KafkaPayload(None, b"", [])
    assert KafkaPayload(b"key", b"value", []) == KafkaPayload(b"key", b"value", [])
    assert KafkaPayload(None, b"", [("key", b"value")]) == KafkaPayload(
        None, b"", [("key", b"value")]
    )
    assert not KafkaPayload(None, b"a", []) == KafkaPayload(None, b"b", [])
    assert not KafkaPayload(b"this", b"", []) == KafkaPayload(b"that", b"", [])
    assert not KafkaPayload(None, b"", [("key", b"this")]) == KafkaPayload(
        None, b"", [("key", b"that")]
    )


def test_payload_pickle_simple() -> None:
    payload = KafkaPayload(b"key", b"value", [])
    assert pickle.loads(pickle.dumps(payload)) == payload


def test_payload_pickle_out_of_band() -> None:
    payload = KafkaPayload(b"key", b"value", [])
    buffers: MutableSequence[PickleBuffer] = []
    data = pickle.dumps(payload, protocol=5, buffer_callback=buffers.append)
    assert pickle.loads(data, buffers=[b.raw() for b in buffers]) == payload


class KafkaStreamsTestCase(StreamsTestMixin[KafkaPayload], TestCase):

    configuration = build_kafka_configuration(
        {"bootstrap.servers": os.environ.get("DEFAULT_BROKERS", "localhost:9092")}
    )

    @contextlib.contextmanager
    def get_topic(self, partitions: int = 1) -> Iterator[Topic]:
        name = f"test-{uuid.uuid1().hex}"
        client = AdminClient(self.configuration)
        [[key, future]] = client.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        ).items()
        assert key == name
        assert future.result() is None
        try:
            yield Topic(name)
        finally:
            [[key, future]] = client.delete_topics([name]).items()
            assert key == name
            assert future.result() is None

    def get_consumer(
        self,
        group: Optional[str] = None,
        enable_end_of_partition: bool = True,
        auto_offset_reset: str = "earliest",
    ) -> KafkaConsumer:
        return KafkaConsumer(
            {
                **self.configuration,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": "false",
                "enable.auto.offset.store": "false",
                "enable.partition.eof": enable_end_of_partition,
                "group.id": group if group is not None else uuid.uuid1().hex,
                "session.timeout.ms": 10000,
            },
        )

    def get_producer(self) -> KafkaProducer:
        return KafkaProducer(self.configuration)

    def get_payloads(self) -> Iterator[KafkaPayload]:
        for i in itertools.count():
            yield KafkaPayload(None, f"{i}".encode("utf8"), [])

    def test_auto_offset_reset_earliest(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="earliest")) as consumer:
                consumer.subscribe([topic])

                message = consumer.poll(10.0)
                assert isinstance(message, Message)
                assert message.offset == 0

    def test_auto_offset_reset_latest(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="latest")) as consumer:
                consumer.subscribe([topic])

                try:
                    consumer.poll(10.0)  # XXX: getting the subcription is slow
                except EndOfPartition as error:
                    assert error.partition == Partition(topic, 0)
                    assert error.offset == 1
                else:
                    raise AssertionError("expected EndOfPartition error")

    def test_auto_offset_reset_error(self) -> None:
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, next(self.get_payloads())).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="error")) as consumer:
                consumer.subscribe([topic])

                with pytest.raises(ConsumerError):
                    consumer.poll(10.0)  # XXX: getting the subcription is slow


def test_commit_codec() -> None:
    commit = Commit("group", Partition(Topic("topic"), 0), 0)
    assert commit_codec.decode(commit_codec.encode(commit)) == commit


def test_as_kafka_configuration_bool() -> None:
    assert as_kafka_configuration_bool(False) == False
    assert as_kafka_configuration_bool("false") == False
    assert as_kafka_configuration_bool("FALSE") == False
    assert as_kafka_configuration_bool("0") == False
    assert as_kafka_configuration_bool("f") == False
    assert as_kafka_configuration_bool(0) == False

    assert as_kafka_configuration_bool(True) == True
    assert as_kafka_configuration_bool("true") == True
    assert as_kafka_configuration_bool("TRUE") == True
    assert as_kafka_configuration_bool("1") == True
    assert as_kafka_configuration_bool("t") == True
    assert as_kafka_configuration_bool(1) == True

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(None)

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("tru")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool("flase")

    with pytest.raises(ValueError):
        assert as_kafka_configuration_bool(2)

    with pytest.raises(TypeError):
        assert as_kafka_configuration_bool(0.0)
