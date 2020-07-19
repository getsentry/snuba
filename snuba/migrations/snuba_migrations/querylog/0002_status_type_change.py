from typing import Sequence

from snuba.clickhouse.columns import Array, Column, Enum, LowCardinality, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.MultiStepMigration):
    """
    Drops the status enum and replaces it with a LowCardinality string
    now that the support for low cardinality strings is better.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                "querylog_local",
                Column("status", LowCardinality(String())),
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                "querylog_local",
                Column("clickhouse_queries.status", Array(LowCardinality(String()))),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        status_type = Enum([("success", 0), ("error", 1), ("rate-limited", 2)])
        return [
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG, "querylog_local", Column("status", status_type),
            ),
            operations.ModifyColumn(
                StorageSetKey.QUERYLOG,
                "querylog_local",
                Column("clickhouse_queries.status", Array(status_type)),
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
