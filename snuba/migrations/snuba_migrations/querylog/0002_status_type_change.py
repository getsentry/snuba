from typing import Sequence

from snuba.clickhouse.columns import Array, Column, Enum, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Drops the status enum and replaces it with a LowCardinality string
    now that the support for low cardinality strings is better.
    """

    blocking = True

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("status", String(Modifiers(low_cardinality=True))),
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column(
                    "clickhouse_queries.status",
                    Array(String(Modifiers(low_cardinality=True))),
                ),
            ),
        ]

    def __backwards_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        status_type = Enum[Modifiers](
            [("success", 0), ("error", 1), ("rate-limited", 2)]
        )
        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG, table_name, Column("status", status_type),
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                Column("clickhouse_queries.status", Array(status_type)),
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
