from dataclasses import dataclass
from threading import Event
from typing import Callable, Mapping, MutableMapping, Optional, Sequence, Set

from snuba.utils.concurrent import Synchronized, execute
from snuba.utils.streams.consumer import Consumer, ConsumerError, EndOfPartition
from snuba.utils.streams.types import Message, Partition, Topic, TPayload


@dataclass(frozen=True)
class Commit:
    __slots__ = ["group", "partition", "offset"]

    group: str
    partition: Partition
    offset: int


class SynchronizedConsumer(Consumer[TPayload]):
    def __init__(
        self,
        consumer: Consumer[TPayload],
        commit_log_consumer: Consumer[Commit],
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

        # TODO: We need to be able to deal with this consumer crashing -- it
        # should then cause the synchronized consumer to be put in an invalid
        # state.
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

            commit = message.payload
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
        # Resume any partitions that can be resumed (where the local
        # offset is less than the remote offset.)
        resume_candidates = set(self.__consumer.paused()) - self.__paused
        if resume_candidates:
            local_offsets = self.tell()
            resume_partitions = []

            with self.__remote_offsets.get() as remote_offsets:
                for partition in resume_candidates:
                    remote_offset = min(
                        offsets.get(partition, 0) for offsets in remote_offsets.values()
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
                offsets.get(message.partition, 0) for offsets in remote_offsets.values()
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
