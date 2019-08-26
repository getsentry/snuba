import logging

from typing import Sequence, Tuple
from confluent_kafka import Consumer, Message, TopicPartition

from snuba.stateful_consumer import StateData, StateOutput
from snuba.stateful_consumer.state_context import State
from snuba.consumers.strict_consumer import CommitDecision, StrictConsumer
from snuba import settings

logger = logging.getLogger('snuba.snapshot-load')


class BootstrapState(State[StateOutput, StateData]):
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

    def handle(self, state_data: StateData) -> Tuple[StateOutput, StateData]:
        output = StateOutput.NO_SNAPSHOT

        logger.info("Runnign Consumer")
        self.__consumer.run()

        logger.info("Caught up on the control topic")
        return (output, None)

    def set_shutdown(self) -> None:
        super(BootstrapState, self).set_shutdown()
        self.__consumer.shutdown()
