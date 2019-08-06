from snuba import settings


def local_dataset_mode():
    return settings.DATASET_MODE == "local"


class TableSchema(object):
    """
    Represents the full set of columns in a clickhouse table, this only contains
    basic metadata for now. The code to generate the schema comes in a followup PR.
    """

    TEST_TABLE_PREFIX = "test_"

    def __init__(self, local_table_name, dist_table_name, columns):
        self.__columns = columns

        self.__local_table_name = local_table_name
        self.__dist_table_name = dist_table_name

    def __make_test_table(self, table_name):
        return table_name if not settings.TESTING else "%s%s" % (self.TEST_TABLE_PREFIX, table_name)

    def get_local_table_name(self):
        """
        This returns the local table name for a distributed environment.
        It is supposed to be used in DDL commands and for maintenance.
        """
        return self.__make_test_table(self.__local_table_name)

    def get_table_name(self):
        """
        This represents the table we interact with to send queries to Clickhouse.
        In distributed mode this will be a distributed table. In local mode it is a local table.
        """
        table_name = self.__local_table_name if local_dataset_mode() else self.__dist_table_name
        return self.__make_test_table(table_name)

    def _get_table_definition(self, name, engine):
        return """
        CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
            'columns': self.__columns.for_schema(),
            'engine': engine,
            'name': name,
        }

    def get_local_table_definition(self):
        return self._get_table_definition(
            self.get_local_table_name(),
            self._get_local_engine()
        )

    def _get_local_engine(self):
        raise NotImplementedError

    def get_columns(self):
        return self.__columns


class MergeTreeSchema(TableSchema):

    def __init__(self, local_table_name, dist_table_name, columns,
            order_by, partition_by, sample_expr=None, settings=None):
        super(MergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name)
        self.__order_by = order_by
        self.__partition_by = partition_by
        self.__sample_expr = sample_expr
        self.__settings = settings

    def _get_engine_type(self):
        return "MergeTree()"

    def _get_local_engine(self):
        partition_by_clause = "PARTITION BY %s" % \
            self.__partition_by if self.__partition_by else ''

        sample_clause = "SAMPLE BY %s" % \
            self.__sample_expr if self.__sample_expr else ''

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
            sample_expr=None, settings=None):
        super(ReplacingMergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name,
            order_by=order_by,
            partition_by=partition_by,
            sample_expr=sample_expr,
            settings=settings)
        self.__version_column = version_column

    def _get_engine_type(self):
        return "ReplacingMergeTree(%s)" % self.__version_column
