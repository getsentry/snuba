from snuba.stateful_consumer import StateOutput
from snuba.stateful_consumer.state_context import State
from snuba.consumers.simple_consumer import create_simple_consumer
from snuba import settings

import simplejson as json
import logging
from typing import Any, Sequence, Tuple
from confluent_kafka import TopicPartition

logger = logging.getLogger('snuba.snapshot-load')


class BootstrapState(State[StateOutput]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def __init__(self,
        topics: str,
        bootstrap_servers: Sequence[str],
        group_id: str,
    ):
        self.__consumer = create_simple_consumer(
            topics,
            bootstrap_servers,
            group_id,
            auto_offset_reset="earliest"
        )

    def handle(self, input: Any) -> Tuple[StateOutput, Any]:
        expected_product = settings.SNAPSHOT_LOAD_PRODUCT
        output = StateOutput.NO_SNAPSHOT
        current_snapshots = FoundSnapshots()

        partitions = self.__consumer.assignment()
        assert len(partitions) == 1, \
            "CDC can support one partition only"
        # TODO: properly manage assignment and revoke. This is not robust right
        # now. It is not guaranteed that by the time we are here we will have a
        # partition.
        watermark = partitions[0].offset

        message = self.__consumer.consume(1)
        while message:
            value = json.loads(message.value())
            message = self.__consumer.consume(1)

        return (output, None)
