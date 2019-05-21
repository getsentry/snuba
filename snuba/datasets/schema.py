from snuba import settings


def local_dataset_mode():
    return settings.DATASET_MODE == "local"


class TableSchema(object):
    """
    Represents the full set of columns in a clickhouse table, this only contains
    basic metadata for now. The code to generate the schema comes in a followup PR.
    """

    TEST_TABLE_PREFIX = "test_"

    def __init__(self, local_table_name, dist_table_name):
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
