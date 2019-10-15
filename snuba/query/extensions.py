from abc import ABC, abstractstaticmethod

from typing import Any, Generic, Mapping, TypeVar

from snuba.query.query_processor import (
    DummyExtensionProcessor,
    QueryProcessor,
)
from snuba.schemas import Schema

TExtensionPayload = TypeVar("TExtensionPayload")


class QueryExtension(ABC, Generic[TExtensionPayload]):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    def __init__(self,
        schema: Schema,
        processor: QueryProcessor[TExtensionPayload],
    ) -> None:
        self.__schema = schema
        self.__processor = processor

    def get_schema(self) -> Schema:
        return self.__schema

    @abstractstaticmethod
    def parse_payload(cls, payload: Mapping[str, Any]) -> TExtensionPayload:
        raise NotImplementedError

    def get_processor(self) -> QueryProcessor[TExtensionPayload]:
        return self.__processor
