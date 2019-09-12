from abc import ABC

from snuba.query.query_processor import (
    DummyExtensionProcessor,
    ExtensionQueryProcessor
)
from snuba.schemas import (
    Schema
)


class QueryExtension(ABC):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    def __init__(self,
        schema: Schema,
        processor: ExtensionQueryProcessor,
    ) -> None:
        self.__schema = schema
        self.__processor = processor

    def get_schema(self) -> Schema:
        return self.__schema

    def get_processor(self) -> ExtensionQueryProcessor:
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


class ProjectExtension(QueryExtension):
    def __init__(self) -> None:
        super().__init__(
            schema=PROJECT_EXTENSION_SCHEMA,
            processor=DummyExtensionProcessor(),
        )
