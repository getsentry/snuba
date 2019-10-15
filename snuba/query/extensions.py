from abc import ABC, abstractmethod
from deprecation import deprecated
from typing import Any, Generic, Mapping, TypeVar

from snuba.request.request_settings import RequestSettings
from snuba.query.query import Query
from snuba.query.query_processor import QueryExtensionProcessor
from snuba.schemas import Schema

TExtensionPayload = TypeVar("TExtensionPayload")


class QueryExtensionData(Generic[TExtensionPayload]):
    def __init__(
        self,
        payload: TExtensionPayload,
        processor: QueryExtensionProcessor[TExtensionPayload],
        # this is deprecated, and will be removed as soon as we remove
        # the last few dependencies on the extension body.
        raw_data: Mapping[str, Any],
    ):
        self.__payload = payload
        self.__processor = processor
        self.__raw_data = raw_data

    def get_payload(self) -> TExtensionPayload:
        return self.__payload

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        self.__processor.process_query(query, self.__payload, request_settings)

    @deprecated(
        details="Do not access the raw extension data. "
        "Extensions should just make changes to the query through process_query")
    def get_raw_data(self) -> Mapping[str, Any]:
        return self.__raw_data


class QueryExtension(ABC, Generic[TExtensionPayload]):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    def __init__(self,
        schema: Schema,
        processor: QueryExtensionProcessor[TExtensionPayload],
    ) -> None:
        self.__schema = schema
        self.__processor = processor

    def validate(self, payload: Mapping[str, Any]) -> QueryExtensionData:
        parsed_payload = self._parse_payload(payload)
        return QueryExtensionData(parsed_payload, self.__processor, payload)

    @abstractmethod
    def _parse_payload(cls, payload: Mapping[str, Any]) -> TExtensionPayload:
        raise NotImplementedError

    def get_schema(self) -> Schema:
        return self.__schema
