import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

import structlog

from snuba.clickhouse.columns import Column
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseNode,
    ClickhouseNodeType,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.table_engines import TableEngine

logger = structlog.get_logger().bind(module=__name__)


class OperationTarget(Enum):
    """
    Represents the target nodes of an operation.
    Either local, distributed.
    """

    LOCAL = "local"
    DISTRIBUTED = "distributed"
    UNSET = "unset"  # target is not set. will throw an error if executed


class SqlOperation(ABC):
    def __init__(
        self,
        storage_set: StorageSetKey,
        target: OperationTarget,
        settings: Optional[Mapping[str, Any]] = None,
    ):
        self._storage_set = storage_set
        self._settings = settings
        self.target = target

    @property
    def storage_set(self) -> StorageSetKey:
        return self._storage_set

    def get_nodes(self) -> Sequence[ClickhouseNode]:
        cluster = get_cluster(self._storage_set)
        local_nodes, dist_nodes = (
            cluster.get_local_nodes(),
            cluster.get_distributed_nodes(),
        )

        if self.target == OperationTarget.LOCAL:
            nodes = local_nodes
        elif self.target == OperationTarget.DISTRIBUTED:
            nodes = dist_nodes
        else:
            raise ValueError(f"Target not set for {self}")
        return nodes

    def execute(self) -> None:
        nodes = self.get_nodes()
        cluster = get_cluster(self._storage_set)
        if nodes:
            logger.info(f"Executing op: {self.format_sql()[:32]}...")
        for node in nodes:
            connection = cluster.get_node_connection(
                ClickhouseClientSettings.MIGRATE, node
            )
            logger.info(f"Executing on {self.target.value} node: {node}")
            connection.execute(self.format_sql(), settings=self._settings)

    @abstractmethod
    def format_sql(self) -> str:
        raise NotImplementedError


class RunSql(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        statement: str,
        target: OperationTarget = OperationTarget.UNSET,
    ) -> None:
        super().__init__(storage_set, target=target)
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
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.table_name = table_name
        self.__columns = columns
        self.engine = engine

    def format_sql(self) -> str:
        columns = ", ".join([col.for_schema() for col in self.__columns])
        cluster = get_cluster(self._storage_set)
        engine = self.engine.get_sql(cluster, self.table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns}) ENGINE {engine};"
        )


class CreateMaterializedView(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        view_name: str,
        destination_table_name: str,
        columns: Sequence[Column[MigrationModifiers]],
        query: str,
        target: OperationTarget = OperationTarget.UNSET,
    ) -> None:
        self.__view_name = view_name
        self.__destination_table_name = destination_table_name
        self.__columns = columns
        self.__query = query
        super().__init__(storage_set, target=target)

    def format_sql(self) -> str:
        columns = ", ".join([col.for_schema() for col in self.__columns])

        return f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.__view_name} TO {self.__destination_table_name} ({columns}) AS {self.__query};"


class RenameTable(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        old_table_name: str,
        new_table_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__old_table_name = old_table_name
        self.__new_table_name = new_table_name

    def format_sql(self) -> str:
        return f"RENAME TABLE {self.__old_table_name} TO {self.__new_table_name};"


class DropTable(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ) -> None:
        super().__init__(storage_set, target=target)
        self.table_name = table_name

    def format_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self.table_name};"


class TruncateTable(SqlOperation):
    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ) -> None:
        super().__init__(storage_set, target=target)
        self.__storage_set = storage_set
        self.__table_name = table_name

    def format_sql(self) -> str:
        return f"TRUNCATE TABLE IF EXISTS {self.__table_name};"


class ModifyTableTTL(SqlOperation):
    """
    Modify TTL of a table
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        reference_column: str,
        ttl_days: int,
        materialize_ttl_on_modify: bool = False,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        self.__materialize_ttl_on_modify = 1 if materialize_ttl_on_modify else 0
        super().__init__(
            storage_set,
            target,
            {"materialize_ttl_on_modify": self.__materialize_ttl_on_modify},
        )
        self.__table_name = table_name
        self.__reference_column = reference_column
        self.__ttl_days = ttl_days

    def format_sql(self) -> str:
        return (
            f"ALTER TABLE {self.__table_name} MODIFY TTL "
            f"{self.__reference_column} + "
            f"toIntervalDay({self.__ttl_days});"
        )


class RemoveTableTTL(SqlOperation):
    """
    Remove TTL from a table.
    NOTE: This cannot be used right now since Clickhouse version 20.3 does not
    support REMOVE TTL command
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__table_name = table_name

    def format_sql(self) -> str:
        return f"ALTER TABLE {self.__table_name} REMOVE TTL;"


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
        after: Optional[str] = None,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.table_name = table_name
        self.column = column
        self.__after = after

    def format_sql(self) -> str:
        column = self.column.for_schema()
        optional_after_clause = f" AFTER {self.__after}" if self.__after else ""
        return f"ALTER TABLE {self.table_name} ADD COLUMN IF NOT EXISTS {column}{optional_after_clause};"


class DropColumn(SqlOperation):
    """
    Drops a column from a table.

    The data from that column will be removed from the filesystem, so this command
    should only be performed once the column is no longer being referenced anywhere.

    You cannot drop a column that is part of the the primary key or the sampling
    key in the engine expression.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        column_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.table_name = table_name
        self.column_name = column_name

    def format_sql(self) -> str:
        return (
            f"ALTER TABLE {self.table_name} DROP COLUMN IF EXISTS {self.column_name};"
        )


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
        ttl_month: Optional[Tuple[str, int]] = None,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__table_name = table_name
        self.__column = column
        self.__ttl_month = ttl_month

    def format_sql(self) -> str:
        column = self.__column.for_schema()
        return f"ALTER TABLE {self.__table_name} MODIFY COLUMN {column}{self.optional_ttl_clause};"

    def get_column(self) -> Column[MigrationModifiers]:
        return self.__column

    def get_table_name(self) -> str:
        return self.__table_name

    @property
    def optional_ttl_clause(self) -> str:
        if self.__ttl_month is None:
            return ""

        ttl_column, ttl_month = self.__ttl_month
        return f" TTL {ttl_column} + INTERVAL {ttl_month} MONTH"


class ModifyTableSettings(SqlOperation):
    """
    Modify the settings of a table.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        settings: Mapping[str, Any],
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__table_name = table_name
        self.__settings = settings

    def format_sql(self) -> str:
        settings = ", ".join(f"{k} = {v}" for k, v in self.__settings.items())
        return f"ALTER TABLE {self.__table_name} MODIFY SETTING {settings};"


class ResetTableSettings(SqlOperation):
    """
    Reset the settings of a table to the default values.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        settings: Sequence[str],
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__table_name = table_name
        self.__settings = settings

    def format_sql(self) -> str:
        settings = ", ".join(self.__settings)
        return f"ALTER TABLE {self.__table_name} RESET SETTING {settings};"


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
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__table_name = table_name
        self.__index_name = index_name
        self.__index_expression = index_expression
        self.__index_type = index_type
        self.__granularity = granularity
        self.__after = after

    def format_sql(self) -> str:
        optional_after_clause = f" AFTER {self.__after}" if self.__after else ""
        return f"ALTER TABLE {self.__table_name} ADD INDEX IF NOT EXISTS {self.__index_name} {self.__index_expression} TYPE {self.__index_type} GRANULARITY {self.__granularity}{optional_after_clause};"


class DropIndex(SqlOperation):
    """
    Drops an index.
    """

    def __init__(
        self,
        storage_set: StorageSetKey,
        table_name: str,
        index_name: str,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
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
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where: Optional[str] = None,
        target: OperationTarget = OperationTarget.UNSET,
    ):
        super().__init__(storage_set, target=target)
        self.__dest_table_name = dest_table_name
        self.__dest_columns = dest_columns
        self.__src_table_name = src_table_name
        self.__src_columns = src_columns
        self.__prewhere = prewhere
        self.__order_by = order_by
        self.__limit = limit
        self.__offset = offset
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

        limit_clause = f" LIMIT {self.__limit}" if self.__limit else ""
        order_by_clause = f" ORDER BY {self.__order_by}" if self.__order_by else ""
        offset_clause = f" OFFSET {self.__offset}" if self.__offset else ""

        return (
            f"INSERT INTO {self.__dest_table_name} ({dest_columns}) SELECT {src_columns} FROM {self.__src_table_name}{prewhere_clause}{where_clause}"
            + f"{order_by_clause}{limit_clause}{offset_clause};"
        )


class GenericMigration(ABC):
    @abstractmethod
    def execute(self, logger: logging.Logger) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_new_node(
        self,
        storage_sets: Sequence[StorageSetKey],
        node_type: ClickhouseNodeType,
        clickhouse: ClickhousePool,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def description(self) -> Optional[str]:
        raise NotImplementedError


class RunPython(GenericMigration):
    """
    `new_node_func` should only be provided in the (probably rare)
    scenario where there is a Python script that must be rerun anytime
    another ClickHouse node is added to the cluster.
    """

    def __init__(
        self,
        func: Callable[[logging.Logger], None],
        new_node_func: Optional[Callable[[Sequence[StorageSetKey]], None]] = None,
        description: Optional[str] = None,
    ) -> None:
        self.__func = func
        self.__new_node_func = new_node_func
        self.__description = description

    def execute(self, logger: logging.Logger) -> None:
        self.__func(logger)

    def execute_new_node(
        self,
        storage_sets: Sequence[StorageSetKey],
        node_type: ClickhouseNodeType,
        clickhouse: ClickhousePool,
    ) -> None:
        if self.__new_node_func is not None:
            self.__new_node_func(storage_sets)

    def description(self) -> Optional[str]:
        return self.__description


class RunSqlAsCode(GenericMigration):
    def __init__(self, operation: SqlOperation) -> None:
        assert isinstance(operation, SqlOperation)
        assert operation.target != OperationTarget.UNSET
        self.__operation = operation

    def execute(self, logger: logging.Logger) -> None:
        self.__operation.execute()

    def execute_new_node(
        self,
        storage_sets: Sequence[StorageSetKey],
        node_type: ClickhouseNodeType,
        clickhouse: ClickhousePool,
    ) -> None:
        if node_type == ClickhouseNodeType.LOCAL:
            if self.__operation.target != OperationTarget.LOCAL:
                return
        else:
            if self.__operation.target != OperationTarget.DISTRIBUTED:
                return

        if self.__operation._storage_set in storage_sets:
            sql = self.__operation.format_sql()
            logger.info(f"Executing {sql}")
            clickhouse.execute(sql)

    def description(self) -> Optional[str]:
        return self.__operation.format_sql()
