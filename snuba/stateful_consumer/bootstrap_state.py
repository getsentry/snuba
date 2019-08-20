from snuba.stateful_consumer import StateType
from snuba.stateful_consumer.state_context import State

from typing import Any, Tuple


class BootstrapState(State[StateType]):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def handle(self, input: Any) -> Tuple[StateType, Any]:
        # TODO: Actually do the snapshot bootstrap
        return (StateType.CONSUMING, None)
