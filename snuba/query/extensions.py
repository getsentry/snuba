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


PROJECT_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        'project': {
            'anyOf': [
                {'type': 'integer', 'minimum': 1},
                {
                    'type': 'array',
                    'items': {'type': 'integer', 'minimum': 1},
                    'minItems': 1,
                },
            ]
        },
    },
    # Need to select down to the project level for customer isolation and performance
    'required': ['project'],
    'additionalProperties': False,
}

PERFORMANCE_EXTENSION_SCHEMA = {
    'type': 'object',
    'properties': {
        # Never add FINAL to queries, enable sampling
        'turbo': {
            'type': 'boolean',
            'default': False,
        },
        # Force queries to hit the first shard replica, ensuring the query
        # sees data that was written before the query. This burdens the
        # first replica, so should only be used when absolutely necessary.
        'consistent': {
            'type': 'boolean',
            'default': False,
        },
        'debug': {
            'type': 'boolean',
            'default': False,
        },
    },
    'additionalProperties': False,
}


class PerformanceExtension(QueryExtension[Any]):
    def __init__(self) -> None:
        super().__init__(
            schema=PERFORMANCE_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )

    def parse_payload(cls, payload: Mapping[str, Any]) -> Any:
        return payload


class ProjectExtension(QueryExtension[Any]):
    def __init__(self) -> None:
        super().__init__(
            schema=PROJECT_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )

    def parse_payload(cls, payload: Mapping[str, Any]) -> Any:
        return payload
