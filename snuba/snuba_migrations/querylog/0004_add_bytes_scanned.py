from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Adds fields for query profile.
    """

    blocking = True

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.bytes_scanned",
                    Array(
                        UInt(64),
                    ),
                ),
                after="clickhouse_queries.array_join_columns",
            ),
        ]

    def __backwards_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.bytes_scanned"
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("querylog_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("querylog_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("querylog_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("querylog_dist")
