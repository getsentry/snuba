from abc import ABC, abstractmethod
from typing import Optional, Sequence


from snuba.clickhouse.columns import Column
from snuba.clusters.cluster import ClickhouseClientSettings, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.table_engines import TableEngine


class Operation(ABC):
    """
    Executed on all the nodes of the cluster.
    """

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError


class SqlOperation(Operation, ABC):
    def __init__(self, storage_set: StorageSetKey):
        self._storage_set = storage_set

    def execute(self) -> None:
        cluster = get_cluster(self._storage_set)

        for node in cluster.get_local_nodes():
            connection = cluster.get_node_connection(
                ClickhouseClientSettings.MIGRATE, node
            )
            connection.execute(self.format_sql())

    @abstractmethod
    def format_sql(self) -> str:
        raise NotImplementedError


class RunSql(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, statement: str) -> None:
        super().__init__(storage_set)
        self.statement = statement

    def format_sql(self) -> str:
        return self.statement


class DropTable(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, table_name: str) -> None:
        super().__init__(storage_set)
        self.table_name = table_name

    def format_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self.table_name};"


class CreateTable(SqlOperation):
    """
    The create table operation takes a table name, column list and table engine.
    All other clauses (e.g. ORDER BY, PARTITION BY, SETTINGS) are parameters to
    engine as they are specific to the ClickHouse table engine selected.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        columns: Sequence[Column],
        engine: TableEngine,
    ):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__columns = columns
        self.__engine = engine

    def format_sql(self) -> str:
        columns = ", ".join([col.for_schema() for col in self.__columns])
        engine = self.__engine.get_sql(
            get_cluster(self._storage_set).is_single_node(), self.__table_name
        )

        return f"CREATE TABLE IF NOT EXISTS {self.__table_name} ({columns}) ENGINE {engine};"


class CreateMaterializedView(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        view_name: str,
        destination_table_name: str,
        columns: Sequence[Column],
        query: str,
    ) -> None:
        self.__view_name = view_name
        self.__destination_table_name = destination_table_name
        self.__columns = columns
        self.__query = query
        super().__init__(storage_set)

    def format_sql(self) -> str:
        columns = ", ".join([col.for_schema() for col in self.__columns])

        return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.__view_name} TO {self.__destination_table_name} ({columns}) AS {self.__query};"


class AddColumn(SqlOperation):
    """
    Adds a column to a table.

    The `after` value represents the name of the existing column after which the
    new column will be added. If no value is passed, it will be added in the last
    position. There is no way to add a column at the start of the table.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        column: Column,
        after: Optional[str],
    ):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__column = column
        self.__after = after

    def format_sql(self) -> str:
        column = self.__column.for_schema()
        optional_after_clause = f" AFTER {self.__after}" if self.__after else ""
        return f"ALTER TABLE {self.__table_name} ADD COLUMN IF NOT EXISTS {column}{optional_after_clause};"


class DropColumn(SqlOperation):
    """
    Drops a column from a table.

    The data from that column will be removed from the filesystem, so this command
    shouuld only be performed once the column is no longer referenced anywhere else.

    You cannot drop a column that is part of the the primary key or the sampling
    key in the engine expression.
    """

    def __init__(self, storage_set: StorageSetKey, table_name: str, column_name: str):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__column_name = column_name

    def format_sql(self) -> str:
        return f"ALTER TABLE {self.__table_name} DROP COLUMN IF EXISTS {self.__column_name};"


class ModifyColumn(SqlOperation):
    """
    Modify a column in a table.

    For columns that are included in the primary key, you can only change the type
    if it doesn't modify the data. e.g. You can add values to an Enum or change a
    type from DateTime to UInt32.
    """

    def __init__(self, storage_set: StorageSetKey, table_name: str, column: Column):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__column = column

    def format_sql(self) -> str:
        column = self.__column.for_schema()
        return f"ALTER TABLE {self.__table_name} MODIFY COLUMN {column};"


class AddIndex(SqlOperation):
    """
    Adds an index.

    Only works with the MergeTree family of tables.

    In ClickHouse versions prior to 20.1.2.4, this requires setting
    allow_experimental_data_skipping_indices = 1
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        index_name: str,
        index_expression: str,
        index_type: str,
        granularity: int,
        after: Optional[str],
    ):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__index_name = index_name
        self.__index_expression = index_expression
        self.__index_type = index_type
        self.__granularity = granularity
        self.__after = after

    def format_sql(self) -> str:
        optional_after_clause = f" AFTER {self.__after}" if self.__after else ""
        return f"ALTER TABLE {self.__table_name} ADD INDEX {self.__index_name} {self.__index_expression} TYPE {self.__index_type} GRANULARITY {self.__granularity}{optional_after_clause};"


class DropIndex(SqlOperation):
    """
    Drops an index.
    """

    def __init__(self, storage_set: StorageSetKey, table_name: str, index_name: str):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__index_name = index_name

    def format_sql(self) -> str:
        return f"ALTER TABLE {self.__table_name} DROP INDEX {self.__index_name};"
