from abc import abstractmethod, ABC

from snuba import settings


def local_dataset_mode():
    return settings.DATASET_MODE == "local"


class SchemaStorage(ABC):
    """
    Abstraction that represent the clickhouse source to be
    provided by dataset schema objects.
    This can represent a single table or a join between mutliple
    tables.

    In this implementation the class itself is able to generate
    the Clickhouse portion of the query (whether it is a SELECT or
    INSERT statement).
    Later (when we will have an abstract schema and a clickhouse one)
    we will split the abstract definition from the Clickhouse
    code generation.
    """

    TEST_TABLE_PREFIX = "test_"

    @abstractmethod
    def for_query(self) -> str:
        raise NotImplementedError


class TableSchemaStorage(SchemaStorage):
    def __init__(self,
        local_table_name: str,
        dist_table_name: str
    ) -> None:
        self.__local_table_name = local_table_name
        self.__dist_table_name = dist_table_name

    def for_query(self) -> str:
        return self.get_table_name()

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
