from typing import Callable, Mapping, List, Sequence

from snuba import settings
from snuba.clickhouse.columns import ColumnSet


def local_dataset_mode():
    return settings.DATASET_MODE == "local"


class Schema(object):
    """
    Represents the full set of columns in a clickhouse table, this only contains
    basic metadata for now.
    """

    TEST_TABLE_PREFIX = "test_"

    def __init__(self, local_table_name, dist_table_name, columns, migration_function=None):
        self.__columns = columns

        self.__local_table_name = local_table_name
        self.__dist_table_name = dist_table_name
        self.__migration_function = migration_function if migration_function else lambda schema: []

    def _make_test_table(self, table_name):
        return table_name if not settings.TESTING else "%s%s" % (self.TEST_TABLE_PREFIX, table_name)

    def get_local_table_name(self):
        """
        This returns the local table name for a distributed environment.
        It is supposed to be used in DDL commands and for maintenance.
        """
        return self._make_test_table(self.__local_table_name)

    def get_table_name(self):
        """
        This represents the table we interact with to send queries to Clickhouse.
        In distributed mode this will be a distributed table. In local mode it is a local table.
        """
        table_name = self.__local_table_name if local_dataset_mode() else self.__dist_table_name
        return self._make_test_table(table_name)

    def get_local_table_definition(self):
        raise NotImplementedError

    def get_local_drop_table_statement(self):
        return "DROP TABLE IF EXISTS %s" % self.get_local_table_name()

    def get_columns(self):
        return self.__columns

    def get_column_differences(self, expected_columns: Mapping[str, str]) -> List[str]:
        """
        Returns a list of differences between the expected_columns and the columns described in the schema.
        """
        errors: List[str] = []

        for column_name, column_type in expected_columns.items():
            if column_name not in self.__columns:
                errors.append("Column '%s' exists in local ClickHouse but not in schema!" % column_name)
                continue

            expected_type = self.__columns[column_name].type.for_schema()
            if column_type != expected_type:
                errors.append(
                    "Column '%s' type differs between local ClickHouse and schema! (expected: %s, is: %s)" % (
                        column_name,
                        expected_type,
                        column_type
                    )
                )

        return errors

    def get_migration_statements(
        self
    ) -> Callable[[str, Mapping[str, str]], Sequence[str]]:
        return self.__migration_function


class TableSchema(Schema):
    def _get_table_definition(self, name: str, engine: str) -> str:
        return """
        CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
            'columns': self.get_columns().for_schema(),
            'engine': engine,
            'name': name,
        }

    def get_local_table_definition(self) -> str:
        return self._get_table_definition(
            self.get_local_table_name(),
            self._get_local_engine()
        )

    def _get_local_engine(self):
        raise NotImplementedError


class MergeTreeSchema(TableSchema):

    def __init__(self, local_table_name, dist_table_name, columns,
            order_by, partition_by, sample_expr=None, settings=None,
            migration_function=None):
        super(MergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name,
            migration_function=migration_function)
        self.__order_by = order_by
        self.__partition_by = partition_by
        self.__sample_expr = sample_expr
        self.__settings = settings

    def _get_engine_type(self):
        return "MergeTree()"

    def _get_local_engine(self):
        partition_by_clause = ("PARTITION BY %s" %
            self.__partition_by) if self.__partition_by else ''

        sample_clause = ("SAMPLE BY %s" %
            self.__sample_expr) if self.__sample_expr else ''

        if self.__settings:
            settings_list = ["%s=%s" % (k, v) for k, v in self.__settings.items()]
            settings_clause = "SETTINGS %s" % ", ".join(settings_list)
        else:
            settings_clause = ''

        return """
            %(engine_type)s
            %(partition_by_clause)s
            ORDER BY %(order_by)s
            %(sample_clause)s
            %(settings_clause)s;""" % {
            'engine_type': self._get_engine_type(),
            'order_by': self.__order_by,
            'partition_by_clause': partition_by_clause,
            'sample_clause': sample_clause,
            'settings_clause': settings_clause,
        }


class ReplacingMergeTreeSchema(MergeTreeSchema):

    def __init__(self, local_table_name, dist_table_name, columns,
            order_by, partition_by, version_column,
            sample_expr=None, settings=None, migration_function=None):
        super(ReplacingMergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name,
            order_by=order_by,
            partition_by=partition_by,
            sample_expr=sample_expr,
            settings=settings,
            migration_function=migration_function)
        self.__version_column = version_column

    def _get_engine_type(self):
        return "ReplacingMergeTree(%s)" % self.__version_column


class SummingMergeTreeSchema(MergeTreeSchema):

    def _get_engine_type(self):
        return "SummingMergeTree()"


class MaterializedViewSchema(Schema):

    def __init__(
            self,
            local_materialized_view_name: str,
            dist_materialized_view_name: str,
            columns: ColumnSet,
            query: str,
            local_source_table_name: str,
            local_destination_table_name: str,
            dist_source_table_name: str,
            dist_destination_table_name: str
    ) -> None:
        super().__init__(
            columns=columns,
            local_table_name=local_materialized_view_name,
            dist_table_name=dist_materialized_view_name,
        )

        # Make sure the caller has provided a source_table_name in the query
        assert query % {'source_table_name': local_source_table_name} != query

        self.__query = query
        self.__local_source_table_name = local_source_table_name
        self.__local_destination_table_name = local_destination_table_name
        self.__dist_source_table_name = dist_source_table_name
        self.__dist_destination_table_name = dist_destination_table_name

    def __get_local_source_table_name(self) -> str:
        return self._make_test_table(self.__local_source_table_name)

    def __get_local_destination_table_name(self) -> str:
        return self._make_test_table(self.__local_destination_table_name)

    def __get_table_definition(self, name: str, source_table_name: str, destination_table_name: str) -> str:
        return """
        CREATE MATERIALIZED VIEW IF NOT EXISTS %(name)s TO %(destination_table_name)s (%(columns)s) AS %(query)s""" % {
            'name': name,
            'destination_table_name': destination_table_name,
            'columns': self.get_columns().for_schema(),
            'query': self.__query,
        } % {
            'source_table_name': source_table_name,
        }

    def get_local_table_definition(self) -> str:
        return self.__get_table_definition(
            self.get_local_table_name(),
            self.__get_local_source_table_name(),
            self.__get_local_destination_table_name()
        )
