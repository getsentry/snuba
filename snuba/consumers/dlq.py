from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional, TypeVar

import rapidjson
from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Message

from snuba.datasets.storages.storage_key import StorageKey
from snuba.redis import RedisClientKey, get_redis_client

redis_client = get_redis_client(RedisClientKey.DLQ)
DLQ_REDIS_KEY = "dlq_instruction"


logger = logging.getLogger(__name__)


class DlqReplayPolicy(Enum):
    STOP_ON_ERROR = "stop-on-error"
    REINSERT_DLQ = "reinsert-dlq"
    DROP_INVALID_MESSAGES = "drop-invalid-messages"


class DlqInstructionStatus(Enum):
    NOT_STARTED = "not-started"
    IN_PROGRESS = "in-progress"


@dataclass
class DlqInstruction:
    """
    The DlqInstruction is a mechanism to notify the DLQ consumer to begin processing
    messages on a particular DLQ in line with the specified policy. It is set from
    Snuba admin and periodically checked for updates by the DLQ consumer.
    """

    policy: DlqReplayPolicy
    status: DlqInstructionStatus
    storage_key: StorageKey
    slice_id: Optional[int]
    max_messages_to_process: int

    def to_bytes(self) -> bytes:
        encoded: str = rapidjson.dumps(
            {
                "policy": self.policy.value,
                "status": self.status.value,
                "storage_key": self.storage_key.value,
                "slice_id": self.slice_id,
                "max_messages_to_process": self.max_messages_to_process,
            }
        )
        return encoded.encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> DlqInstruction:
        decoded = rapidjson.loads(raw.decode("utf-8"))

        return cls(
            policy=DlqReplayPolicy(decoded["policy"]),
            status=DlqInstructionStatus(decoded["status"]),
            storage_key=StorageKey(decoded["storage_key"]),
            slice_id=decoded["slice_id"],
            max_messages_to_process=decoded["max_messages_to_process"],
        )

    def is_valid(self) -> bool:
        """
        Replaying topics with post processing enabled is not yet supported.
        This will be supported in a future iteratrion and this code can be removed.
        """
        return self.storage_key.value not in ("errors", "transactions", "search_issues")


def load_instruction() -> Optional[DlqInstruction]:
    value = redis_client.get(DLQ_REDIS_KEY)

    if value is None:
        return None

    return DlqInstruction.from_bytes(value)


def mark_instruction_in_progress() -> None:
    """
    Mark the current instruction as in progress. Not atomic.
    """
    instruction = load_instruction()
    if instruction:
        instruction.status = DlqInstructionStatus.IN_PROGRESS
        store_instruction(instruction)


def clear_instruction() -> None:
    redis_client.delete(DLQ_REDIS_KEY)


def store_instruction(instruction: DlqInstruction) -> None:
    redis_client.set(DLQ_REDIS_KEY, instruction.to_bytes())


TPayload = TypeVar("TPayload")


class ExitAfterNMessages(ProcessingStrategy[TPayload]):
    """
    Forwards messages until N messages is reached, then forces the
    consumer to close. This is used by the DLQ consumer
    which is expected to process a fixed number of messages requested
    by the user.

    If max_timeout is hit, the consumer also exits.
    """

    def __init__(
        self,
        next_step: ProcessingStrategy[TPayload],
        num_messages_to_process: int,
        max_message_timeout: float,
    ) -> None:
        self.__num_messages_to_process = num_messages_to_process
        self.__processed_messages = 0
        self.__next_step = next_step
        self.__last_message_time = time.time()
        self.__max_message_timeout = max_message_timeout
        self.__exiting = False

    def __exit(self) -> None:
        if self.__exiting:
            return

        self.__exiting = True
        logger.info("Processed %d messages", self.__processed_messages)
        signal.raise_signal(signal.SIGINT)

    def poll(self) -> None:
        self.__next_step.poll()
        if self.__last_message_time + self.__max_message_timeout < time.time():
            self.__exit()

        if self.__processed_messages >= self.__num_messages_to_process:
            self.__exit()

    def submit(self, message: Message[TPayload]) -> None:
        if self.__processed_messages < self.__num_messages_to_process:
            self.__last_message_time = time.time()

            try:
                self.__next_step.submit(message)
            except InvalidMessage:
                self.__processed_messages += 1
                raise
            self.__processed_messages += 1

    def close(self) -> None:
        if self.__processed_messages < self.__num_messages_to_process:
            logger.warning(
                "Closing DLQ consumer after %d messages", self.__processed_messages
            )
        self.__next_step.close()

    def terminate(self) -> None:
        if self.__processed_messages < self.__num_messages_to_process:
            logger.warning(
                "Closing DLQ consumer after %d messages", self.__processed_messages
            )
        self.__next_step.terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)
