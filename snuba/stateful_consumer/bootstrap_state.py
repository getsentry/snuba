from snuba.stateful_consumer.consumer_context import State, StateType

from typing import Any


class BootstrapState(State):
    """
    This is the state the consumer starts into.
    Its job is to either transition to normal operation or
    to recover a previously running snapshot if the conumer
    was restarted while the process was on going.
    The recovery process is done by consuming the whole
    control topic.
    """

    def _handle_impl(self, input: Any) -> (StateType, Any):
        # TODO: Actually do the snapshot bootstrap
        return (StateType.CONSUMING, None)
