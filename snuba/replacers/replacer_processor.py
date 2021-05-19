from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, NamedTuple, Optional, TypeVar

from snuba.datasets.schemas.tables import WritableTableSchema


class ReplacementMessage(NamedTuple):
    """
    Represent a generic replacement message (version 2 in our protocol) that we
    find on the replacement topic.
    TODO: We should use codecs to encode/decode kafka replacements messages.
    """

    action_type: str  # This is a string to make this class agnostic to the dataset
    data: Mapping[str, Any]


class Replacement(ABC):
    @abstractmethod
    def get_insert_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()

    @abstractmethod
    def get_count_query(self, table_name: str) -> Optional[str]:
        raise NotImplementedError()


R = TypeVar("R", bound=Replacement)


class ReplacerProcessor(ABC, Generic[R]):
    """
    Processes one message from the replacer topic into a data structure that contains
    the query to apply the replacement.
    Every dataset/storage that needs to implement replacements, needs to provide an
    instance of this class that will be used by the ReplacementWorker.
    """

    def __init__(self, schema: WritableTableSchema) -> None:
        self.__schema = schema

    @abstractmethod
    def process_message(self, message: ReplacementMessage) -> Optional[R]:
        """
        Processes one message from the topic.
        """
        raise NotImplementedError

    def get_schema(self) -> WritableTableSchema:
        return self.__schema

    def pre_replacement(self, replacement: R, matching_records: int) -> bool:
        """
        Custom actions to run before the replacements when we already know how
        many rows will be impacted.
        """
        return False

    def post_replacement(
        self, replacement: R, duration: int, matching_records: int
    ) -> None:
        """
        Custom actions to run after the replacement was executed.
        """
        pass
