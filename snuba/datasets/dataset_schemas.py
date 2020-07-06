from typing import Optional, List, Sequence, Union

from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import DDLStatement, TableSchema, WritableTableSchema


class StorageSchemas(object):
    """
    A collection of schemas associated with a Storage, providing access to schemas and DDL functions
    """

    def __init__(
        self,
        read_schema: Schema,
        write_schema: Union[WritableTableSchema, None],
        intermediary_schemas: Optional[List[Schema]] = None,
    ) -> None:
        if intermediary_schemas is None:
            intermediary_schemas = []

        self.__read_schema = read_schema
        self.__write_schema = write_schema
        self.__intermediary_schemas = intermediary_schemas

    def get_read_schema(self) -> Schema:
        return self.__read_schema

    def get_write_schema(self) -> Optional[WritableTableSchema]:
        return self.__write_schema

    def _get_unique_schemas(self) -> Sequence[Schema]:
        unique_schemas: List[Schema] = []

        all_schemas_with_possible_duplicates = [self.__read_schema]
        if self.__write_schema:
            all_schemas_with_possible_duplicates.append(self.__write_schema)
        all_schemas_with_possible_duplicates.extend(self.__intermediary_schemas)

        for schema in all_schemas_with_possible_duplicates:
            if schema not in unique_schemas:
                unique_schemas.append(schema)

        return unique_schemas

    def get_create_statements(self) -> Sequence[DDLStatement]:
        return [
            schema.get_local_table_definition()
            for schema in self._get_unique_schemas()
            if isinstance(schema, TableSchema)
        ]

    def get_drop_statements(self) -> Sequence[DDLStatement]:
        return [
            schema.get_local_drop_table_statement()
            for schema in self._get_unique_schemas()
            if isinstance(schema, TableSchema)
        ]
