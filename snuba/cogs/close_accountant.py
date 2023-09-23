from __future__ import annotations

from typing import TypeVar

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import Message

from snuba.cogs.accountant import close_cogs_recorder

TPayload = TypeVar("TPayload")


class CloseCOGSProducer(ProcessingStrategy[TPayload]):
    def __init__(self, next_step: ProcessingStrategy[TPayload]) -> None:
        self.__next_step = next_step

    def poll(self) -> None:
        self.__next_step.poll()

    def submit(self, message: Message[TPayload]) -> None:
        self.__next_step.submit(message)

    def close(self) -> None:
        close_cogs_recorder()
        self.__next_step.close()

    def terminate(self) -> None:
        close_cogs_recorder(force=True)
        self.__next_step.terminate()

    def join(self, timeout: float | None = None) -> None:
        self.__next_step.join(timeout)
