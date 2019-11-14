from __future__ import annotations

import logging
import signal
import time
import uuid
from concurrent.futures import FIRST_EXCEPTION, wait
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from random import Random
from threading import Event, Lock
from typing import (
    Any,
    Callable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

from confluent_kafka import Consumer, KafkaError, Message, OFFSET_INVALID
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.concurrent import execute

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class Stream:
    topic: str
    partition: int


@dataclass
class Offsets:
    local: Optional[int]
    remote: MutableMapping[str, Optional[int]]


class Subscriptions:
    def handle(self, message: Message) -> Subscriptions:
        raise NotImplementedError


class LoadingSubscriptions(Subscriptions):
    def handle(
        self, message: Message
    ) -> Union[LoadingSubscriptions, StreamingSubscriptions]:
        error = message.error()
        if error is not None:
            if error == KafkaError._PARTITION_EOF:
                return StreamingSubscriptions()
            else:
                raise Exception(error)

        raise NotImplementedError

        return self


class StreamingSubscriptions(Subscriptions):
    def handle(self, message: Message) -> StreamingSubscriptions:
        error = message.error()
        if error is not None:
            if error == KafkaError._PARTITION_EOF:
                return self
            else:
                raise Exception(error)

        raise NotImplementedError

        return self


@dataclass
class StreamState:
    offsets: Offsets
    subscriptions: Subscriptions


class StreamStateManager:
    def __init__(self) -> None:
        self.__streams: MutableMapping[Stream, StreamState] = {}
        self.__assignment_callbacks: Sequence[
            Callable[[Mapping[Stream, StreamState]], None]
        ] = []
        self.__revocation_callbacks: Sequence[
            Callable[[Mapping[Stream, StreamState]], None]
        ] = []
        self.__lock = Lock()

    def add_callbacks(
        self,
        on_assign: Optional[Callable[[Mapping[Stream, StreamState]], None]] = None,
        on_revoke: Optional[Callable[[Mapping[Stream, StreamState]], None]] = None,
    ) -> None:
        with self.__lock:
            if on_assign is not None:
                self.__assignment_callbacks.append(on_assign)
                if self.__streams:
                    on_assign(self.__streams)

            if on_revoke is not None:
                self.__revocation_callbacks.append(on_revoke)

    def assign(self, streams: Sequence[Stream, StreamState]) -> None:
        with self.__lock:
            logger.debug("Received stream assignment: %r", streams)

            assert not self.__streams

            self.__streams = streams

            logger.debug("Invoking stream assignment callbacks...")
            for callback in self.__assignment_callbacks:
                callback(self.__streams)

    def revoke(self, streams: Sequence[Stream]) -> None:
        with self.__lock:
            logger.debug("Recieved stream revocation: %r", streams)

            assert set(streams) == self.__streams.keys()

            logger.debug("Invoking stream revocation callbacks...")
            for callback in self.__revocation_callbacks:
                callback(self.__streams)

            self.__streams.clear()

    @contextmanager
    def get(self, stream: Stream) -> Iterator[StreamState]:
        with self.__lock:
            yield self.__streams[stream]


class CommitLogConsumer:
    def __init__(
        self,
        kafka_configuration: Mapping[str, Any],
        consumer_group: str,
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__kafka_configuration = kafka_configuration
        self.__consumer_group = consumer_group
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                **self.__kafka_configuration,
                "group.id": self.__consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
            }
        )

        def on_assign(streams: MutableMapping[Stream, StreamState]) -> None:
            # XXX: This is not thread safe -- this happens in the query
            # executor thread, and will need to be pushed back to the
            # commit log consumer thread instead.
            logger.debug("Updating commit log consumer assignment...")

            # TODO: This transformation should be configurable.
            commit_log_streams = set(
                [
                    Stream(f"{stream.topic}-commit-log", stream.partition)
                    for stream in streams.keys()
                ]
            )

            logger.debug("Assigning %r to %r...", sorted(commit_log_streams), consumer)
            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition)
                    for stream in commit_log_streams
                ]
            )

        self.__stream_state_manager.add_callbacks(on_assign=on_assign)

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise Exception(error)

            [topic, partition, group] = message.key().decode("utf-8").split(":")
            offset = int(message.value().decode("utf-8"))

            stream = Stream(topic, int(partition))
            with self.__stream_state_manager.get(stream) as state:
                state.offsets.remote[group] = offset

    def run(self) -> Future[None]:
        return execute(self.__run, name="commit-log-consumer")


class SubscriptionConsumer:
    def __init__(
        self,
        kafka_configuration: Mapping[str, Any],
        consumer_group: str,
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__kafka_configuration = kafka_configuration
        self.__consumer_group = consumer_group
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                **self.__kafka_configuration,
                "group.id": self.__consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": "beginning",
                "enable.partition.eof": True,
            }
        )

        stream_mapping: MutableMapping[Stream, Stream] = {}

        def on_assign(streams: MutableMapping[Stream, StreamState]) -> None:
            # XXX: This is not thread safe -- this happens in the query
            # executor thread, and will need to be pushed back to the
            # subscription consumer thread instead.
            logger.debug("Updating subscription consumer assignment...")

            # TODO: This transformation should be configurable.
            stream_mapping.update(
                {
                    Stream(f"{stream.topic}-subscriptions", stream.partition): stream
                    for stream in streams.keys()
                }
            )

            logger.debug(
                "Assigning %r to %r...", sorted(stream_mapping.keys()), consumer
            )
            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition)
                    for stream in stream_mapping.keys()
                ]
            )

        def on_revoke(streams: MutableMapping[Stream, StreamState]) -> None:
            logger.debug("Revoking subscription consumer assignment...")
            consumer.unassign()
            stream_mapping.clear()

        self.__stream_state_manager.add_callbacks(
            on_assign=on_assign, on_revoke=on_revoke
        )

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            if message.topic() is not None and message.partition() is not None:
                stream = Stream(message.topic(), message.partition())
                with self.__stream_state_manager.get(stream_mapping[stream]) as state:
                    state.subscriptions.handle(message)
            else:
                error = message.error()
                assert error is not None
                raise Exception(error)

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscriptions-consumer")


class SubscribedQueryExecutionConsumer:
    def __init__(
        self,
        kafka_configuration: Mapping[str, Any],
        topic: str,
        consumer_group: str,
        remote_consumer_groups: Sequence[str],
        stream_state_manager: StreamStateManager,
        shutdown_requested: Event,
    ) -> None:
        self.__kafka_configuration = kafka_configuration
        self.__topic = topic
        self.__consumer_group = consumer_group
        self.__remote_consumer_groups = remote_consumer_groups
        self.__stream_state_manager = stream_state_manager
        self.__shutdown_requested = shutdown_requested

    def __run(self) -> None:
        logger.debug("Starting %r...", self)

        consumer = Consumer(
            {
                **self.__kafka_configuration,
                "group.id": self.__consumer_group,
                "auto.offset.reset": "error",
                "enable.auto.commit": False,
            }
        )

        def on_assign(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            streams: MutableMapping[Stream, StreamState] = {}

            for partition in consumer.committed(partitions):
                # TODO: The starting offset should be configurable.
                # TODO: This seems like the wrong place to instantiate the
                # remote consumer groups, but not sure where else it'd go...
                streams[Stream(partition.topic, partition.partition)] = StreamState(
                    offsets=Offsets(
                        local=partition.offset
                        if partition.offset != OFFSET_INVALID
                        else consumer.get_watermark_offsets(partition)[0],
                        remote={group: None for group in self.__remote_consumer_groups},
                    ),
                    subscriptions=LoadingSubscriptions(),
                )

            consumer.assign(
                [
                    ConfluentTopicPartition(
                        stream.topic, stream.partition, state.offsets.local
                    )
                    for stream, state in streams.items()
                ]
            )

            self.__stream_state_manager.assign(streams)

        def on_revoke(
            consumer: Consumer, partitions: Sequence[ConfluentTopicPartition]
        ) -> None:
            self.__stream_state_manager.revoke(
                [
                    Stream(partition.topic, partition.partition)
                    for partition in partitions
                ]
            )

        logger.debug("Subscribing to %r...", self.__topic)
        consumer.subscribe([self.__topic], on_assign=on_assign, on_revoke=on_revoke)

        while not self.__shutdown_requested.is_set():
            message: Optional[Message] = consumer.poll(0.1)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise Exception(error)

            stream = Stream(message.topic(), message.partition())
            with self.__stream_state_manager.get(stream) as state:
                state.offsets.local = message.offset() + 1

    def run(self) -> Future[None]:
        return execute(self.__run, name="subscribed-query-execution-consumer")


if __name__ == "__main__":
    import click

    @dataclass
    class Environment:
        namespace: str

        def get_kafka_configuration(self):
            return {
                "bootstrap.servers": "localhost:9092",
            }

        def get_topic(self) -> str:
            return f"{self.namespace}"

        def get_commit_log_topic(self) -> str:
            return f"{self.namespace}-commit-log"

        def get_subscription_topic(self) -> str:
            return f"{self.namespace}-subscriptions"

    @click.group()
    @click.pass_context
    @click.option("--log-level", type=str, default="debug")
    @click.option("--namespace", type=str, default=None)
    def cli(context, *, log_level: str, namespace: Optional[str]) -> None:
        if namespace is None:
            namespace = f"test-{uuid.uuid1().hex}"

        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s %(levelname)-8s %(threadName)s %(message)s",
        )

        context.obj = Environment(namespace)

    @cli.command()
    @click.pass_context
    @click.option("--partitions", type=int, default=3)
    @click.option("--replication-factor", type=int, default=1)
    def setup(context, *, partitions: int, replication_factor: int) -> None:
        environment: Environment = context.obj

        from confluent_kafka.admin import AdminClient, NewTopic

        client = AdminClient({**environment.get_kafka_configuration()})

        topics = [
            environment.get_topic(),
            environment.get_commit_log_topic(),
            environment.get_subscription_topic(),
        ]

        click.echo("Creating topics...", err=True)
        futures = client.create_topics(
            [NewTopic(topic, partitions, replication_factor) for topic in topics]
        )
        wait(futures.values())
        for topic, future in futures.items():
            click.echo(f"{topic}: {future}", err=True)

        click.echo(environment.namespace)

        click.echo("Waiting for signal...", err=True)
        try:
            signal.pause()
        except KeyboardInterrupt:
            pass

        click.echo("Deleting topics...", err=True)
        futures = client.delete_topics(topics)
        wait(futures.values())
        for topic, future in futures.items():
            click.echo(f"{topic}: {future}", err=True)

    @cli.command()
    @click.pass_context
    @click.option("--commit-probability", type=float, default=0.002)
    @click.option("--consumer-group", "-g", "consumer_groups", type=str, multiple=True)
    @click.option("--seed", type=str)
    def generator(
        context,
        *,
        commit_probability: float,
        consumer_groups: Sequence[str],
        seed: Optional[str] = None,
    ) -> None:
        environment: Environment = context.obj

        assert 1.0 >= commit_probability >= 0.0

        if seed is None:
            seed = f"{hex(int(time.time()))}"

        random = Random(seed)
        logger.debug("Instantiated random number generator with seed: %r", seed)

        from confluent_kafka import Producer

        producer = Producer({**environment.get_kafka_configuration()})

        partitions = [
            *producer.list_topics().topics[environment.get_topic()].partitions.keys()
        ]

        def produce(*args, **kwargs):
            queued = False
            while not queued:
                try:
                    producer.produce(*args, **kwargs)
                except BufferError as error:
                    logger.debug(
                        "Could not produce message due to error, will retry...",
                        exc_info=True,
                    )
                    producer.flush()
                else:
                    queued = True

        def on_delivery(
            error, message: Message, commit_consumer_groups: Sequence[str]
        ) -> None:
            if error is not None:
                raise Exception(error)

            for group in commit_consumer_groups:
                logger.debug(
                    "Committing offset (%r: %r) for group %r...",
                    Stream(message.topic(), message.partition()),
                    message.offset(),
                    group,
                )
                produce(
                    environment.get_commit_log_topic(),
                    key=f"{message.topic()}:{message.partition()}:{group}".encode(
                        "utf-8"
                    ),
                    value=f"{message.offset()}".encode("utf-8"),
                    partition=message.partition(),
                )

        while True:
            produce(
                environment.get_topic(),
                partition=random.choice(partitions),
                on_delivery=partial(
                    on_delivery,
                    commit_consumer_groups=[
                        group
                        for group in consumer_groups
                        if commit_probability > random.random()
                    ],
                ),
            )

    @cli.command()
    @click.pass_context
    @click.option("--consumer-group", type=str, default="snuba-subscriptions")
    def consumer(context, *, consumer_group: str) -> None:
        environment: Environment = context.obj

        shutdown_requested = Event()

        stream_state_manager = StreamStateManager()

        # XXX: This will not type check -- these need a common type.
        futures = {
            consumer.run(): consumer
            for consumer in [
                CommitLogConsumer(
                    environment.get_kafka_configuration(),
                    consumer_group,
                    stream_state_manager,
                    shutdown_requested,
                ),
                SubscriptionConsumer(
                    environment.get_kafka_configuration(),
                    consumer_group,
                    stream_state_manager,
                    shutdown_requested,
                ),
                SubscribedQueryExecutionConsumer(
                    environment.get_kafka_configuration(),
                    environment.get_topic(),
                    consumer_group,
                    [],  # TODO
                    stream_state_manager,
                    shutdown_requested,
                ),
            ]
        }

        def handler(signal, frame):
            logger.debug("Caught signal %r, requesting shutdown...", signal)
            shutdown_requested.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        done, running = wait(futures.keys(), return_when=FIRST_EXCEPTION)

        if not shutdown_requested.is_set():
            logger.warning("Requesting early shutdown due to %r...", done)
            shutdown_requested.set()

        for future, consumer in futures.items():
            try:
                result = future.result()
                logger.debug(
                    "%r completed successfully, returning: %s", consumer, result
                )
            except Exception as error:
                logger.exception(
                    "%r completed with error, raising: %s", consumer, error
                )

    cli()
