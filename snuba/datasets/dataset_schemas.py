from typing import Optional, List, Sequence

from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import (
    DDLStatement,
    TableSchemaWithDDL,
    WritableTableSchema,
)


class StorageSchemas(object):
    """
    A collection of schemas associated with a Storage, providing access to schemas and DDL functions
    """

    def __init__(
        self, schema: Schema, intermediary_schemas: Optional[List[Schema]] = None,
    ) -> None:
        if intermediary_schemas is None:
            intermediary_schemas = []

        self.__schema = schema
        self.__intermediary_schemas = intermediary_schemas

    def get_read_schema(self) -> Schema:
        return self.__schema

    def get_write_schema(self) -> Optional[WritableTableSchema]:
        if isinstance(self.__schema, WritableTableSchema):
            return self.__schema
        else:
            return None

    def get_unique_schemas(self) -> Sequence[Schema]:
        return [self.__schema] + self.__intermediary_schemas

    def get_create_statements(self) -> Sequence[DDLStatement]:
        return [
            schema.get_local_table_definition()
            for schema in self.get_unique_schemas()
            if isinstance(schema, TableSchemaWithDDL)
        ]
