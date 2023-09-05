from __future__ import annotations

import os
from abc import abstractmethod
from typing import Any, Optional, Type, cast

from snuba.consumers.types import KafkaMessageMetadata
from snuba.processor import MessageProcessor, ProcessedMessage
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class MessageProcessorUnsupportedFromConfig(Exception):
    pass


class DatasetMessageProcessor(MessageProcessor, metaclass=RegisteredClass):
    """
    The Processor is responsible for converting an incoming message body from the
    event stream into a row or statement to be inserted or executed against clickhouse.

    The DatasetMessageProcessor is responsible for the message processors that write data
    for a storage.
    """

    def __init__(self) -> None:
        pass

    @classmethod
    def from_name(cls, name: str) -> DatasetMessageProcessor:
        return cast(Type["DatasetMessageProcessor"], cls.class_from_name(name))()

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @abstractmethod
    def process_message(
        self, message: Any, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        raise NotImplementedError


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.datasets.processors"
)
