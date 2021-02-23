from abc import ABC, abstractmethod
from typing import Callable, Optional, Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.table_engines import TableEngine


class SqlOperation(ABC):
    def __init__(self, storage_set: StorageSetKey):
        self._storage_set = storage_set

    @property
    def storage_set(self) -> StorageSetKey:
        return self._storage_set

    def execute(self, local: bool) -> None:
        cluster = get_cluster(self._storage_set)

        nodes = cluster.get_local_nodes() if local else cluster.get_distributed_nodes()

        for node in nodes:
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
        self.__statement = statement

    def format_sql(self) -> str:
        return self.__statement


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
        columns: Sequence[Column[MigrationModifiers]],
        engine: TableEngine,
    ):
        super().__init__(storage_set)
        self.__table_name = table_name
        self.__columns = columns
        self.__engine = engine

    def format_sql(self) -> str:
        columns = ", ".join([col.for_schema() for col in self.__columns])
        cluster = get_cluster(self._storage_set)
        engine = self.__engine.get_sql(cluster, self.__table_name)

        return f"CREATE TABLE IF NOT EXISTS {self.__table_name} ({columns}) ENGINE {engine};"


class CreateMaterializedView(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        view_name: str,
        destination_table_name: str,
        columns: Sequence[Column[MigrationModifiers]],
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


class RenameTable(SqlOperation):
    def __init__(
        self, storage_set: StorageSetKey, old_table_name: str, new_table_name: str,
    ):
        super().__init__(storage_set)
        self.__old_table_name = old_table_name
        self.__new_table_name = new_table_name

    def format_sql(self) -> str:
        return f"RENAME TABLE {self.__old_table_name} TO {self.__new_table_name};"


class DropTable(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, table_name: str) -> None:
        super().__init__(storage_set)
        self.table_name = table_name

    def format_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self.table_name};"


class TruncateTable(SqlOperation):
    def __init__(self, storage_set: StorageSetKey, table_name: str):
        super().__init__(storage_set)
        self.__storage_set = storage_set
        self.__table_name = table_name

    def format_sql(self) -> str:
        return f"TRUNCATE TABLE IF EXISTS {self.__table_name};"


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
        column: Column[MigrationModifiers],
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
    should only be performed once the column is no longer being referenced anywhere.

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

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        column: Column[MigrationModifiers],
    ):
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
        after: Optional[str] = None,
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
        return (
            f"ALTER TABLE {self.__table_name} DROP INDEX IF EXISTS {self.__index_name};"
        )


class InsertIntoSelect(SqlOperation):
    """
    Inserts the results of a select query. Source and destination tables must be
    on the same storage set (and cluster). Data is inserted from src_columns to
    dest_columns based on the order of the src and dest columns provided.

    This operation may not be very performant if data is inserted into several partitions
    at once. It may be better to group data by partition key and insert in batches.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        dest_table_name: str,
        dest_columns: Sequence[str],
        src_table_name: str,
        src_columns: Sequence[str],
        prewhere: Optional[str] = None,
        where: Optional[str] = None,
    ):
        super().__init__(storage_set)
        self.__dest_table_name = dest_table_name
        self.__dest_columns = dest_columns
        self.__src_table_name = src_table_name
        self.__src_columns = src_columns
        self.__prewhere = prewhere
        self.__where = where

    def format_sql(self) -> str:
        src_columns = ", ".join(self.__src_columns)
        dest_columns = ", ".join(self.__dest_columns)

        if self.__prewhere:
            prewhere_clause = f" PREWHERE {self.__prewhere}"
        else:
            prewhere_clause = ""

        if self.__where:
            where_clause = f" WHERE {self.__where}"
        else:
            where_clause = ""

        return f"INSERT INTO {self.__dest_table_name} ({dest_columns}) SELECT {src_columns} FROM {self.__src_table_name}{prewhere_clause}{where_clause};"


class RunPython:
    def __init__(
        self, func: Callable[[], None], description: Optional[str] = None
    ) -> None:
        self.__func = func
        self.__description = description

    def execute(self) -> None:
        self.__func()

    def description(self) -> Optional[str]:
        return self.__description
