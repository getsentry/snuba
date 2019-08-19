from snuba.stateful_consumer.consumer_context import State, StateType
from snuba.consumers.simple_consumer import create_simple_consumer
from snuba.stateful_consumer.control_protocol import (
    parse_control_message,
    SnapshotInit,
    SnapshotLoaded,
)
from snuba import settings

import simplejson as json
from typing import Any, Sequence


class BootstrapState(State):
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

    def _handle_impl(self, input: Any) -> (StateType, Any):
        expected_product = settings.SNAPSHOT_LOAD_PRODUCT
        next_state = StateType.CONSUMING
        message = self.__consumer.consume(1)

        while message:
            value = json.loads(message.value())
            parsed_message = parse_control_message(value)
            if parsed_message.product == expected_product:
                if isinstance(parsed_message, SnapshotInit):
                    next_state = StateType.SNAPSHOT_PAUSED
                elif isinstance(parsed_message, SnapshotLoaded):
                    next_state = StateType.CATCHING_UP
        # TODO: commit the previous message

        return (next_state, None)
