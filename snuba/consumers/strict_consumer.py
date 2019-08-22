from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from enum import Enum
from typing import Callable, Sequence

import logging

logger = logging.getLogger('snuba.kafka-consumer')


class NoPartitionAssigned(Exception):
    pass


class CommitDecision(Enum):
    # Go ahead consuming and do not commit for now
    DO_NOT_COMMIT = 0
    # Commit the offset you consumed before this current message
    COMMIT_PREV = 1
    # Commit this offset
    COMMIT_THIS = 2


class StrictConsumer:
    """
    This is a simple Kafka consumer that consumes messages without batching
    from the commit point to the end of the topic and then terminates.
    It waits for a partition to be assigned before actually consuming. If no
    partition is assigned within a timeout it fails.
    It also moves the responsibility of deciding what to commnit to the user
    compared to the BatchingConsumer

    This is not supposed to be used for continuously consume a topic but to
    catch up a topic from the beginning to the state it is at the time we start.
    An example is the control topic when taking snapshots.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
        auto_offset_reset: str,
        partition_assignment_timeout: int,
        on_partitions_assigned: Callable[[Consumer, Sequence[TopicPartition]], None],
        on_partitions_revoked: Callable[[Consumer, Sequence[TopicPartition]], None],
        on_message: Callable[[Message], CommitDecision],
    ) -> None:
        self.__on_partitions_assigned = on_partitions_assigned
        self.__on_partitions_revoked = on_partitions_revoked
        self.__on_message = on_message
        self._assign_timeout = partition_assignment_timeout

        self.__shutdown = False

        consumer_config = {
            'enable.auto.commit': False,
            'bootstrap.servers': ','.join(bootstrap_servers),
            'group.id': group_id,
            'enable.partition.eof': 'true',
            'default.topic.config': {
                'auto.offset.reset': auto_offset_reset,
            },
        }

        self.__consumer = self.create_consumer(consumer_config)

        def on_partitions_assigned(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            logger.info("New partitions assigned: %r", partitions)
            self.__on_partitions_assigned(consumer, partitions)

        def on_partitions_revoked(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            logger.info("Partitions revoked: %r", partitions)
            self.__on_partitions_revoked(consumer, partitions)

        self.__consumer.subscribe(
            [topic],
            on_assign=on_partitions_assigned,
            on_revoke=on_partitions_revoked,
        )

    def create_consumer(self, config) -> Consumer:
        return Consumer(config)

    def run(self) -> None:
        watermarks = {}

        message = self.__consumer.poll(timeout=self._assign_timeout)
        if not message:
            # Since we enabled enable.partition.eof, no message means I could not
            # consume and it is different from the case I managed to consume and
            # I reached the end of the partition. Thus we fail.
            raise NoPartitionAssigned("No partition was assigned within timeout")

        while not self.__shutdown:
            error = message.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of topic reached")
                    self.__consumer.close()
                    return
                else:
                    raise Exception(message.error())

            commit_decision = self.__on_message(message)
            if commit_decision == CommitDecision.COMMIT_THIS:
                self.__consumer.commit()
            elif commit_decision == CommitDecision.COMMIT_PREV:
                prev_watermark = watermarks.get(
                    (message.partition(), message.topic()),
                )
                if prev_watermark is not None:
                    commit_pos = TopicPartition(
                        partition=message.partition(),
                        topic=message.topic(),
                        offset=prev_watermark,
                    )
                    self.__consumer.commit(commit_pos)
                else:
                    logger.debug(
                        "No previous message to commit on partition %s on topic %s",
                        message.partition(),
                        message.topic(),
                    )

            watermarks[(message.partition(), message.topic())] = message.offset()
            message = self.__consumer.poll(timeout=1.0)

        self.__consumer.close()

    def force_commit(self, partition: TopicPartition) -> None:
        self.__consumer.commit(partition)

    def shutdown(self) -> None:
        logger.debug("Shutting down Strinct Consumer")
        self.__shutdown = True
