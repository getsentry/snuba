from typing import Optional, List, Sequence

from snuba.datasets.schema import Schema, TableSchema


class DatasetSchemas(object):
    """
    A collection of schemas associated with a dataset, providing access to schemas and DDL functions
    """

    def __init__(
            self,
            read_schema: Schema,
            write_schema: TableSchema,
            intermediary_schemas: Optional[List[Schema]] = None
    ) -> None:
        if intermediary_schemas is None:
            intermediary_schemas = []

        self.__read_schema = read_schema
        self.__write_schema = write_schema
        self.__intermediary_schemas = intermediary_schemas

    def get_read_schema(self) -> Schema:
        return self.__read_schema

    def get_write_schema(self) -> TableSchema:
        return self.__write_schema

    def __get_unique_schemas(self) -> List[Schema]:
        unique_schemas: List[Schema] = []
        all_schemas_with_possible_duplicates = [self.__read_schema, self.__write_schema] + self.__intermediary_schemas

        for schema in all_schemas_with_possible_duplicates:
            if schema not in unique_schemas:
                unique_schemas.append(schema)

        return unique_schemas

    def get_create_statements(self) -> Sequence[str]:
        return [
            schema.get_local_table_definition()
            for schema in self.__get_unique_schemas()
            if isinstance(schema, TableSchema)
        ]

    def get_drop_statements(self) -> Sequence[str]:
        return [
            schema.get_local_drop_table_statement()
            for schema in self.__get_unique_schemas()
            if isinstance(schema, TableSchema)
        ]
