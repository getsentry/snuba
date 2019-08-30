import logging

from typing import Sequence, Tuple
from confluent_kafka import Consumer, Message, TopicPartition

from snuba import settings
from snuba.consumers.strict_consumer import CommitDecision, StrictConsumer
from snuba.stateful_consumer import ConsumerStateData, ConsumerStateCompletionEvent
from snuba.utils.state_machine import State


logger = logging.getLogger('snuba.snapshot-load')


class BootstrapState(State[ConsumerStateCompletionEvent, ConsumerStateData]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def __init__(self,
        topic: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ):
        super(BootstrapState, self).__init__()

        def on_partitions_assigned(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            pass

        def on_partitions_revoked(
            consumer: Consumer,
            partitions: Sequence[TopicPartition],
        ):
            pass

        self.__consumer = StrictConsumer(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            partition_assignment_timeout=settings.SNAPSHOT_CONTROL_TOPIC_INIT_TIMEOUT,
            on_partitions_assigned=on_partitions_assigned,
            on_partitions_revoked=on_partitions_revoked,
            on_message=self.__handle_msg,
        )

    def __handle_msg(self, message: Message) -> CommitDecision:
        logger.info(
            "MSG %r %r %r",
            message,
            message.value(),
            message.error(),
        )
        # TODO: Actually do something with the messages and drive the
        # state machine to the next state.
        return CommitDecision.DO_NOT_COMMIT

    def signal_shutdown(self) -> None:
        self.__consumer.shutdown()

    def handle(self, state_data: ConsumerStateData) -> Tuple[ConsumerStateCompletionEvent, ConsumerStateData]:
        logger.info("Running Consumer")
        self.__consumer.run()

        logger.info("Caught up on the control topic")
        return (
            ConsumerStateCompletionEvent.NO_SNAPSHOT,
            ConsumerStateData.no_snapshot_state(),
        )
