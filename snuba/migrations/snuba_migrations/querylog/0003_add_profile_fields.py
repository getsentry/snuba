from typing import Sequence

from snuba.clickhouse.columns import Array, Column, LowCardinality, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.MultiStepMigration):
    """
    Adds fields for query profile.
    """

    blocking = True

    def __forward_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column("clickhouse_queries.time_range", Array(UInt(16))),
                after="clickhouse_queries.consistent",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.all_columns",
                    Array(Array(LowCardinality(String()))),
                ),
                after="clickhouse_queries.time_range",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column("clickhouse_queries.or_conditions", Array(UInt(8))),
                after="clickhouse_queries.all_columns",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.where_columns",
                    Array(Array(LowCardinality(String()))),
                ),
                after="clickhouse_queries.or_conditions",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.where_mapping_columns",
                    Array(Array(LowCardinality(String()))),
                ),
                after="clickhouse_queries.where_columns",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.groupby_columns",
                    Array(Array(LowCardinality(String()))),
                ),
                after="clickhouse_queries.where_mapping_columns",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name=table_name,
                column=Column(
                    "clickhouse_queries.array_join_columns",
                    Array(Array(LowCardinality(String()))),
                ),
                after="clickhouse_queries.groupby_columns",
            ),
        ]

    def __backwards_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.time_range"
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.all_columns"
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.or_conditions"
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.where_columns"
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                "clickhouse_queries.where_mapping_columns",
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG, table_name, "clickhouse_queries.groupby_columns"
            ),
            operations.DropColumn(
                StorageSetKey.QUERYLOG,
                table_name,
                "clickhouse_queries.array_join_columns",
            ),
        ]

    def forwards_local(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("querylog_local")

    def backwards_local(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("querylog_local")

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("querylog_dist")

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("querylog_dist")
