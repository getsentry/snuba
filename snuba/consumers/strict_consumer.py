from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from enum import Enum
from typing import Callable, MutableMapping, Optional, Sequence, Tuple

import logging

from snuba.utils.streams.backends.kafka import KafkaBrokerConfig

logger = logging.getLogger("snuba.kafka-consumer")


class NoPartitionAssigned(Exception):
    pass


class PartitionReassigned(Exception):
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
    from the current committed offset to the end of the topic and then terminates.
    It waits for a partition to be assigned before actually consuming. If no
    partition is assigned within a timeout it fails.
    It also moves the responsibility of deciding what to commit to the user
    compared to other stream processing implementations.

    This is not supposed to be used for continuously consume a topic but to
    catch up on a topic from the beginning to the state it is at the time we start.
    An example is the control topic when taking snapshots.
    """

    def __init__(
        self,
        topic: str,
        broker_config: KafkaBrokerConfig,
        group_id: str,
        initial_auto_offset_reset: str,
        partition_assignment_timeout: int,
        on_message: Callable[[Message], CommitDecision],
        on_partitions_assigned: Optional[
            Callable[[Consumer, Sequence[TopicPartition]], None]
        ] = None,
        on_partitions_revoked: Optional[
            Callable[[Consumer, Sequence[TopicPartition]], None]
        ] = None,
    ) -> None:
        self.__on_partitions_assigned = on_partitions_assigned
        self.__on_partitions_revoked = on_partitions_revoked
        self.__on_message = on_message
        self.__assign_timeout = partition_assignment_timeout
        self.__shutdown = False
        self.__topic = topic
        self.__consuming = False

        consumer_config = broker_config.copy()
        consumer_config.update(
            {
                "enable.auto.commit": False,
                "group.id": group_id,
                "enable.partition.eof": "true",
                "auto.offset.reset": initial_auto_offset_reset,
            }
        )

        self.__consumer = self._create_consumer(consumer_config)

        def _on_partitions_assigned(
            consumer: Consumer, partitions: Sequence[TopicPartition],
        ) -> None:
            logger.info("New partitions assigned: %r", partitions)
            if self.__consuming:
                # In order to ensure the topic is consumed front to back
                # we cannot allow assigning a new partition.
                raise PartitionReassigned(
                    "Cannot allow partition reassignment while catching up."
                )

            if self.__on_partitions_assigned:
                self.__on_partitions_assigned(consumer, partitions)

        def _on_partitions_revoked(
            consumer: Consumer, partitions: Sequence[TopicPartition],
        ) -> None:
            logger.info("Partitions revoked: %r", partitions)
            if self.__on_partitions_revoked:
                self.__on_partitions_revoked(consumer, partitions)

        logger.debug(
            "Subscribing strict consumer to topic %s on broker %r",
            topic,
            broker_config.get("bootstrap.servers"),
        )
        self.__consumer.subscribe(
            [topic],
            on_assign=_on_partitions_assigned,
            on_revoke=_on_partitions_revoked,
        )

    def _create_consumer(self, config: KafkaBrokerConfig) -> Consumer:
        return Consumer(config)

    def run(self) -> None:
        logger.debug("Running Strict Consumer")
        partitions_metadata = (
            self.__consumer.list_topics(self.__topic).topics[self.__topic].partitions
        )

        logger.debug("Partitions metadata received %r", partitions_metadata)
        assert (
            len(partitions_metadata) == 1
        ), f"Strict consumer only supports one partition topics. Found {len(partitions_metadata)} partitions"

        watermarks: MutableMapping[Tuple[int, str], int] = {}

        logger.debug("Starting Strict Consumer event loop")
        while not self.__shutdown:
            message = self.__consumer.poll(timeout=self.__assign_timeout)
            self.__consuming = True
            if not message:
                # Since we enabled enable.partition.eof, no message means I could not
                # consume and it is different from the case I managed to consume and
                # I reached the end of the partition. Thus we fail.
                raise NoPartitionAssigned("No partition was assigned within timeout")
            error = message.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
                    logger.debug("End of topic reached")
                    break
                else:
                    raise message.error()

            logger.debug("Processing message %r", message)
            commit_decision = self.__on_message(message)
            if commit_decision == CommitDecision.COMMIT_THIS:
                self.__consumer.commit(asynchronous=False)
            elif commit_decision == CommitDecision.COMMIT_PREV:
                prev_watermark = watermarks.get((message.partition(), message.topic()),)
                if prev_watermark is not None:
                    commit_pos = TopicPartition(
                        partition=message.partition(),
                        topic=message.topic(),
                        offset=prev_watermark,
                    )
                    self.__consumer.commit(offsets=[commit_pos], asynchronous=False)
                else:
                    logger.debug(
                        "No previous message to commit on partition %s on topic %s",
                        message.partition(),
                        message.topic(),
                    )

            watermarks[(message.partition(), message.topic())] = message.offset()

        self.__consuming = False
        self.__consumer.close()

    def signal_shutdown(self) -> None:
        logger.debug("Shutting down %r", self)
        self.__shutdown = True
