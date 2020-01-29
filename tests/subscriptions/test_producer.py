import contextlib
import uuid
from concurrent.futures import Future
from contextlib import closing
from datetime import timedelta
from typing import Iterator, Optional

from confluent_kafka.admin import AdminClient, NewTopic

from snuba.subscriptions.codecs import SubscriptionResult, SubscriptionResultCodec
from snuba.subscriptions.data import (
    PartitionId,
    Subscription,
    SubscriptionData,
)
from snuba.subscriptions.producer import SubscriptionResultProducer
from snuba.subscriptions.subscription import SubscriptionIdentifier
from snuba.subscriptions.task import ScheduledTask
from snuba.utils.streams.kafka import (
    KafkaConsumer,
    KafkaProducer,
)
from snuba.utils.streams.types import Message, Topic


class TestSubscriptionResultProducer:

    configuration = {"bootstrap.servers": "127.0.0.1"}

    codec = SubscriptionResultCodec()

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
    ) -> KafkaConsumer[int]:
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
            self.codec,
        )

    def get_producer(self) -> KafkaProducer[int]:
        return SubscriptionResultProducer(self.configuration, self.codec)

    def test_auto_offset_reset_earliest(self) -> None:

        query_future = Future()
        query_future.set_result(
            {"data": {"count": 100, "max_partition": 1, "max_offset": 2}}
        )

        result = SubscriptionResult(
            ScheduledTask(
                123,
                Subscription(
                    SubscriptionIdentifier(PartitionId(1), uuid.uuid1()),
                    SubscriptionData(
                        1,
                        [],
                        [["count()", "", "count"]],
                        timedelta(minutes=1),
                        timedelta(minutes=1),
                    ),
                ),
            ),
            query_future,
        )
        with self.get_topic() as topic:
            with closing(self.get_producer()) as producer:
                producer.produce(topic, result).result(5.0)

            with closing(self.get_consumer(auto_offset_reset="earliest")) as consumer:
                consumer.subscribe([topic])

                message = consumer.poll(10.0)
                assert isinstance(message, Message)
                assert message.offset == 0
