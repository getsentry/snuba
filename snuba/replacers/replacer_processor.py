from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Generic, Mapping, NamedTuple, Optional, TypeVar

from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import ReplacementType


class ReplacerState(Enum):
    ERRORS = "errors"


class ReplacementMessageMetadata(NamedTuple):
    """
    Metadata from the original Kafka Message for a Replacement Message.
    """

    partition_index: int
    offset: int
    consumer_group: str


class ReplacementMessage(NamedTuple):
    """
    Represent a generic replacement message (version 2 in our protocol) that we
    find on the replacement topic.
    TODO: We should use codecs to encode/decode kafka replacements messages.
    """

    action_type: ReplacementType  # This is a string to make this class agnostic to the dataset
    data: Mapping[str, Any]
    metadata: ReplacementMessageMetadata


class Replacement(ABC):
    @abstractmethod
    def get_insert_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_count_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def should_write_every_node(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_message_metadata(self) -> ReplacementMessageMetadata:
        raise NotImplementedError()


R = TypeVar("R", bound=Replacement)


class ReplacerProcessor(ABC, Generic[R]):
    """
    Processes one message from the replacer topic into a data structure that contains
    the query to apply the replacement.
    Every dataset/storage that needs to implement replacements, needs to provide an
    instance of this class that will be used by the ReplacementWorker.
    """

    @abstractmethod
    def process_message(self, message: ReplacementMessage) -> Optional[R]:
        """
        Processes one message from the topic.
        """
        raise NotImplementedError

    def initialize_schema(self, schema: WritableTableSchema) -> None:
        """
        Schema is initialized by the parent storage after ReplacerProcessor is
        instantiated.

        This method should be called before any processing is done by the processor!
        """
        self.__schema = schema

    def get_schema(self) -> WritableTableSchema:
        return self.__schema

    @abstractmethod
    def get_state(self) -> ReplacerState:
        raise NotImplementedError

    def pre_replacement(self, replacement: R, matching_records: int) -> bool:
        """
        Custom actions to run before the replacements when we already know how
        many rows will be impacted.
        """
        return False

    def post_replacement(self, replacement: R, matching_records: int) -> None:
        """
        Custom actions to run after the replacement was executed.
        """
        pass
