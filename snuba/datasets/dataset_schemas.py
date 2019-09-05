from typing import Optional, List, Iterator

from snuba.datasets.schema import Schema


class DatasetSchemas(object):
    """
    A collection of schemas associated with a dataset, providing access to schemas and aggregated functions
    """

    def __init__(
            self,
            read_schema: Schema,
            write_schema: Schema,
            intermediary_schemas: Optional[List[Schema]] = None
    ) -> None:
        if intermediary_schemas is None:
            intermediary_schemas = []

        self.__read_schema = read_schema
        self.__write_schema = write_schema
        self.__intermediary_schemas = intermediary_schemas

    def get_read_schema(self) -> Schema:
        return self.__read_schema

    def get_write_schema(self) -> Schema:
        return self.__write_schema

    def __get_unique_schemas(self) -> List[Schema]:
        unique_schemas: List[Schema] = []
        all_schemas_with_possible_duplicates = [self.__read_schema, self.__write_schema] + self.__intermediary_schemas

        for schema in all_schemas_with_possible_duplicates:
            if schema not in unique_schemas:
                unique_schemas.append(schema)

        return unique_schemas

    def get_create_statements(self) -> Iterator[str]:
        return map(lambda schema: schema.get_local_table_definition(), self.__get_unique_schemas())

    def get_drop_statements(self) -> Iterator[str]:
        return map(lambda schema: schema.get_local_drop_table_statement(), self.__get_unique_schemas())
