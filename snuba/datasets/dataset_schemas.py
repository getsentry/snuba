class DatasetSchemas(object):
    """
    A collection of tables associated with a dataset, providing access to schemas and aggregated functions
    """

    def __init__(self, read_schema, write_schema, intermediary_schemas=None):
        if intermediary_schemas is None:
            intermediary_schemas = []

        self.__read_schema = read_schema
        self.__write_schema = write_schema
        self.__intermediary_schemas = intermediary_schemas

    def get_read_schema(self):
        return self.__read_schema

    def get_write_schema(self):
        return self.__write_schema

    def __get_unique_schemas(self):
        all_schemas_with_possible_duplicates = [self.__read_schema, self.__write_schema] + self.__intermediary_schemas

        return list(set(all_schemas_with_possible_duplicates))

    def get_create_statements(self):
        return map(lambda schema: schema.get_local_table_definition(), self.__get_unique_schemas())

    def get_drop_statements(self):
        return map(lambda schema: schema.get_local_drop_table_statement(), self.__get_unique_schemas())
