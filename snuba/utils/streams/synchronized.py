from dataclasses import dataclass
from threading import Event
from typing import Callable, Mapping, MutableMapping, Optional, Sequence, Set

from snuba.utils.codecs import Codec
from snuba.utils.concurrent import Synchronized, execute
from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.kafka import KafkaPayload
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset"]

    group: str
    partition: Partition
    offset: int


class CommitCodec(Codec[KafkaPayload, Commit]):
    def encode(self, value: Commit) -> KafkaPayload:
        return KafkaPayload(
            f"{value.partition.topic.name}:{value.partition.index}:{value.group}".encode(
                "utf-8"
            ),
            f"{value.offset}".encode("utf-8"),
        )

    def decode(self, value: KafkaPayload) -> Commit:
        key = value.key
        if not isinstance(key, bytes):
            raise TypeError("payload key must be a bytes object")

        val = value.value
        if not isinstance(val, bytes):
            raise TypeError("payload value must be a bytes object")

        topic_name, partition_index, group = key.decode("utf-8").split(":", 3)
        offset = int(val.decode("utf-8"))
        return Commit(group, Partition(Topic(topic_name), int(partition_index)), offset)


commit_codec = CommitCodec()


class SynchronizedConsumer(Consumer[TPayload]):
    """
    This class implements a consumer that is can only consume messages that
    have already been consumed and committed by one or more other consumer
    groups.

    The consumer groups that are being "followed" are required to publish
    their offsets to a shared commit log topic. The advancement of the
    offsets for these consumer groups in the commit log topic controls
    whether or not the local consumer is allowed to consume messages from its
    assigned partitions. (This commit log topic works similarly to/was
    inspired by/essentially duplicates the contents of the Kafka built-in
    ``__consumer_offsets`` topic, which seems to be intended to be a private
    API of the Kafka system based on the lack of external documentation.)

    It is important to note that the since the local consumer is only allowed
    to consume messages that have been consumed and committed by all of the
    members of the referenced consumer groups, this consumer can only consume
    messages as fast as the slowest consumer (or in other words, the most
    latent or lagging consumer) for each partition. If one of these consumers
    stops consuming messages entirely, this consumer will also stop making
    progress in those partitions.
    """

    def __init__(
        self,
        consumer: Consumer[TPayload],
        commit_log_consumer: Consumer[KafkaPayload],
        commit_log_topic: Topic,
        commit_log_groups: Set[str],
    ) -> None:
        self.__consumer = consumer

        self.__commit_log_consumer = commit_log_consumer
        self.__commit_log_topic = commit_log_topic
        self.__commit_log_groups = commit_log_groups

        self.__remote_offsets: Synchronized[
            Mapping[str, MutableMapping[Partition, int]]
        ] = Synchronized({group: {} for group in commit_log_groups})

        self.__commit_log_worker_stop_requested = Event()
        self.__commit_log_worker = execute(self.__run_commit_log_worker)

        # The set of partitions that have been paused by the caller/user. This
        # takes precedence over whether or not the partition should be paused
        # due to offset synchronization.
        self.__paused: Set[Partition] = set()

    def __run_commit_log_worker(self) -> None:
        # TODO: This needs to roll back to the initial offset.

        # TODO: This needs to ensure that it is subscribed to all partitions.

        self.__commit_log_consumer.subscribe([self.__commit_log_topic])

        while not self.__commit_log_worker_stop_requested.is_set():
            try:
                message = self.__commit_log_consumer.poll(0.1)
            except EndOfPartition:
                continue

            if message is None:
                continue

            commit = commit_codec.decode(message.payload)
            if commit.group not in self.__commit_log_groups:
                continue

            with self.__remote_offsets.get() as remote_offsets:
                # NOTE: This will store data about partitions that are not
                # actually part of the subscription or assignment. This
                # approach (potentially) requires more memory and locking
                # overhead (due to writing state for partitions that are not
                # subscribed or assigned), but amortizes the cost of the
                # initial load of the topic and makes the implementation
                # quite a bit simpler.
                remote_offsets[commit.group][commit.partition] = commit.offset

        self.__commit_log_consumer.close()

    def __check_commit_log_worker_running(self) -> None:
        if not self.closed and not self.__commit_log_worker.running():
            try:
                self.__commit_log_worker.result()
            except Exception as e:
                raise RuntimeError("commit log consumer thread crashed") from e
            else:
                raise RuntimeError("commit log consumer thread unexpectedly exited")

    def subscribe(
        self,
        topics: Sequence[Topic],
        on_assign: Optional[Callable[[Mapping[Partition, int]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[Partition]], None]] = None,
    ) -> None:
        def assignment_callback(offsets: Mapping[Partition, int]) -> None:
            for partition in offsets:
                self.__paused.discard(partition)

            if on_assign is not None:
                on_assign(offsets)

        def revocation_callback(partitions: Sequence[Partition]) -> None:
            for partition in partitions:
                self.__paused.discard(partition)

            if on_revoke is not None:
                on_revoke(partitions)

        return self.__consumer.subscribe(
            topics, on_assign=assignment_callback, on_revoke=revocation_callback
        )

    def unsubscribe(self) -> None:
        return self.__consumer.unsubscribe()

    def poll(self, timeout: Optional[float] = None) -> Optional[Message[TPayload]]:
        self.__check_commit_log_worker_running()

        # Resume any partitions that can be resumed (where the local
        # offset is less than the remote offset.)
        resume_candidates = set(self.__consumer.paused()) - self.__paused
        if resume_candidates:
            local_offsets = self.tell()
            resume_partitions = []

            with self.__remote_offsets.get() as remote_offsets:
                for partition in resume_candidates:
                    remote_offset = min(
                        (
                            offsets.get(partition, 0)
                            for offsets in remote_offsets.values()
                        ),
                        default=0,
                    )
                    if remote_offset > local_offsets[partition]:
                        resume_partitions.append(partition)

            if resume_partitions:
                self.__consumer.resume(resume_partitions)

        # We don't need to explicitly handle ``EndOfPartition`` here -- even if
        # we receive the next message before the leader, we will roll back our
        # offsets and wait for the leader to advance.
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        with self.__remote_offsets.get() as remote_offsets:
            remote_offset = min(
                (
                    offsets.get(message.partition, 0)
                    for offsets in remote_offsets.values()
                ),
                default=0,
            )

        # Check to make sure the message does not exceed the remote offset. If
        # it does, pause the partition and seek back to the message offset.
        if message.offset >= remote_offset:
            self.__consumer.pause([message.partition])
            self.__consumer.seek({message.partition: message.offset})
            return None

        return message

    def pause(self, partitions: Sequence[Partition]) -> None:
        if self.closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.tell().keys():
            raise ConsumerError("cannot pause unassigned partitions")

        for partition in partitions:
            self.__paused.add(partition)

        self.__consumer.pause(partitions)

    def resume(self, partitions: Sequence[Partition]) -> None:
        if self.closed:
            raise RuntimeError("consumer is closed")

        if set(partitions) - self.tell().keys():
            raise ConsumerError("cannot resume unassigned partitions")

        # Partitions are not actually resumed by the inner consumer immediately
        # upon calling this method. Instead, any partitions that are able to be
        # resumed will be resumed at the start of the next ``poll`` call.
        for partition in partitions:
            self.__paused.discard(partition)

    def paused(self) -> Sequence[Partition]:
        return [*self.__paused]

    def tell(self) -> Mapping[Partition, int]:
        return self.__consumer.tell()

    def seek(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.seek(offsets)

    def stage_offsets(self, offsets: Mapping[Partition, int]) -> None:
        return self.__consumer.stage_offsets(offsets)

    def commit_offsets(self) -> Mapping[Partition, int]:
        return self.__consumer.commit_offsets()

    def close(self, timeout: Optional[float] = None) -> None:
        # TODO: Be careful to ensure there are not any deadlock conditions
        # here. Should this actually wait for the commit log worker?
        self.__commit_log_worker_stop_requested.set()
        return self.__consumer.close(timeout)

    @property
    def closed(self) -> bool:
        return self.__consumer.closed
