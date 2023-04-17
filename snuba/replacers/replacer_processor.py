from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Generic, Mapping, Optional, TypeVar, cast

from typing_extensions import NamedTuple

from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.processor import ReplacementType
from snuba.utils.registered_class import RegisteredClass


class ReplacerState(Enum):
    ERRORS = "errors"


class ReplacementMessageMetadata(NamedTuple):
    """
    Metadata from the original Kafka Message for a Replacement Message.
    """

    partition_index: int
    offset: int
    consumer_group: str


T = TypeVar("T")


class ReplacementMessage(NamedTuple, Generic[T]):
    """
    Represent a generic replacement message (version 2 in our protocol) that we
    find on the replacement topic.
    TODO: We should use codecs to encode/decode kafka replacements messages.
    """

    action_type: ReplacementType  # This is a string to make this class agnostic to the dataset
    data: T
    metadata: ReplacementMessageMetadata


class Replacement(ABC):
    @classmethod
    @abstractmethod
    def get_replacement_type(cls) -> ReplacementType:
        raise NotImplementedError()

    @abstractmethod
    def get_insert_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_count_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def should_write_every_node(self) -> bool:
        raise NotImplementedError()


R = TypeVar("R", bound=Replacement)


class ReplacerProcessor(ABC, Generic[R], metaclass=RegisteredClass):
    """
    Processes one message from the replacer topic into a data structure that contains
    the query to apply the replacement.
    Every dataset/storage that needs to implement replacements, needs to provide an
    instance of this class that will be used by the ReplacementWorker.
    """

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "ReplacerProcessor[R]":
        return cls(**kwargs)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> "ReplacerProcessor[R]":
        return cast("ReplacerProcessor[R]", cls.class_from_name(name))

    @abstractmethod
    def process_message(
        self, message: ReplacementMessage[Mapping[str, Any]]
    ) -> Optional[R]:
        """
        Processes one message from the topic.
        """
        raise NotImplementedError

    @abstractmethod
    def get_schema(self) -> WritableTableSchema:
        raise NotImplementedError

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
