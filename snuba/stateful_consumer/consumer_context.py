from functools import partial
from batching_kafka_consumer import BatchingKafkaConsumer

from snuba.datasets import Dataset
from snuba.stateful_consumer import StateData, StateType, StateOutput
from snuba.stateful_consumer.state_context import StateContext
from snuba.stateful_consumer.bootstrap_state import BootstrapState
from snuba.stateful_consumer.consuming_state import ConsumingState
from snuba.stateful_consumer.paused_state import PausedState
from snuba.stateful_consumer.catching_up_state import CatchingUpState

from typing import Mapping, Sequence


class ConsumerContext(StateContext[StateType]):
    """
    Context class for the stateful consumer. The states defined here
    regulate when the consumer is consuming from the main topic and when
    it is consuming from the control topic.
    """

    def __init__(
        self,
        dataset: Dataset,
        dataset_name: str,
        raw_topic: str,
        replacements_topic: str,
        max_batch_size: int,
        max_batch_time_ms: int,
        bootstrap_servers: Sequence[str],
        group_id: str,
        commit_log_topic: str,
        auto_offset_reset: str,
        queued_max_messages_kbytes: int,
        queued_min_messages: int,
        dogstatsd_host: str,
        dogstatsd_port: int,
        control_topic: str,
    ) -> None:
        consumer_builder = partial(
            initialize_batching_consumer,
            dataset=dataset,
            dataset_name=dataset_name,
            raw_topic=raw_topic,
            replacements_topic=replacements_topic,
            max_batch_size=max_batch_size,
            max_batch_time_ms=max_batch_time_ms,
            bootstrap_server=bootstrap_servers,
            group_id=group_id,
            commit_log_topic=commit_log_topic,
            auto_offset_reset=auto_offset_reset,
            queued_max_messages_kbytes=queued_max_messages_kbytes,
            queued_min_messages=queued_min_messages,
            dogstatsd_host=dogstatsd_host,
            dogstatsd_port=dogstatsd_port
        )

        states = {
            StateType.BOOTSTRAP: BootstrapState(
                control_topic,
                bootstrap_servers,
                group_id,
            ),
            StateType.CONSUMING: ConsumingState(consumer_builder),
            StateType.SNAPSHOT_PAUSED: PausedState(),
            StateType.CATCHING_UP: CatchingUpState(consumer_builder),
        }
        start_state = StateType.BOOTSTRAP
        terminal_state = StateType.FINISHED
        super(ConsumerContext, self).__init__(
            states=states,
            start_state=start_state,
            terminal_state=terminal_state
        )

    def _get_state_transitions(self) -> Mapping[StateType, Mapping[StateOutput, StateType]]:
        return {
            StateType.BOOTSTRAP: {
                StateOutput.NO_SNAPSHOT: StateType.CONSUMING,
                StateOutput.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
                StateOutput.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
            },
            StateType.CONSUMING: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_INIT_RECEIVED: StateType.SNAPSHOT_PAUSED,
            },
            StateType.SNAPSHOT_PAUSED: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_READY_RECEIVED: StateType.CATCHING_UP,
            },
            StateType.CATCHING_UP: {
                StateOutput.FINISH: StateType.FINISHED,
                StateOutput.SNAPSHOT_CATCHUP_COMPLETED: StateType.CONSUMING,
            },
        }
