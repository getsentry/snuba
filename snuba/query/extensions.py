from abc import ABC

from snuba.query.query_processor import ExtensionQueryProcessor
from snuba.schemas import Schema


class QueryExtension(ABC):
    """
    Defines a query extension by coupling a query extension schema
    and a query extension processor that updates the query by
    processing the extension data.
    """

    def __init__(self, schema: Schema, processor: ExtensionQueryProcessor,) -> None:
        self.__schema = schema
        self.__processor = processor

    def get_schema(self) -> Schema:
        return self.__schema

    def get_processor(self) -> ExtensionQueryProcessor:
        return self.__processor
