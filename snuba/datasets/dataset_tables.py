import itertools


class DatasetTables(object):
    """
    A collection of tables associated with a dataset, used to obfuscate methods from the schema
    """

    def __init__(self, read_schema, write_schema, intermediary_schemas=[]):
        self.__read_schema = read_schema
        self.__write_schema = write_schema
        self.__intermediary_schemas = intermediary_schemas

    def get_write_schema(self):
        # TODO(manu): remove this
        return self.__write_schema

    def get_read_schema(self):
        # TODO(manu): remove this
        return self.__read_schema

    def get_local_write_table_name(self):
        return self.__write_schema.get_local_table_name()

    def get_local_read_table_name(self):
        return self.__read_schema.get_local_table_name()

    def get_write_table_name(self):
        return self.__write_schema.get_table_name()

    def get_read_table_name(self):
        return self.__read_schema.get_table_name()

    def __get_unique_schemas(self):
        all_schemas_with_possible_duplicates = list(
            itertools.chain.from_iterable([
                [self.__read_schema, self.__write_schema],
                self.__intermediary_schemas]
            )
        )

        return list(set(all_schemas_with_possible_duplicates))

    def get_all_table_names(self):
        # TODO: remove get_all_table_names method once we not longer need it in the health check
        return map(lambda schema: schema.get_table_name(), self.__get_unique_schemas())

    def get_all_local_table_names(self):
        return map(lambda schema: schema.get_local_table_name(), self.__get_unique_schemas())

    def get_create_statements(self):
        return map(lambda schema: schema.get_local_table_definition(), self.__get_unique_schemas())

    def get_drop_statements(self):
        return map(lambda schema: schema.get_local_drop_table_statement(), self.__get_unique_schemas())

    def get_schema_differences(self, local_table_name, expected_columns):
        errors = []
        unique_schemas = self.__get_unique_schemas()
        possible_schemas = [schema for schema in unique_schemas if schema.get_local_table_name() == local_table_name]
        assert len(possible_schemas) == 1

        schema = possible_schemas[0]
        schema_columns = schema.get_columns()

        for column_name, column_type in expected_columns.items():
            if column_name not in schema_columns:
                errors.append("Column '%s' exists in local ClickHouse but not in schema!", column_name)
                continue

            expected_type = schema_columns[column_name].type.for_schema()
            if column_type != expected_type:
                errors.append(
                    "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)",
                    column_name,
                    expected_type,
                    column_type
                )

        return errors
