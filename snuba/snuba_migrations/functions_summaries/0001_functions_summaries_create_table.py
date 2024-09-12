from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Float

storage_set_name = StorageSetKey.FUNCTIONS_SUMMARIES
local_table_name = "functions_summaries_local"
dist_table_name = "functions_summaries_dist"

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    # profile_id is nullable since this will only be used by transaction-based profiling
    Column("profile_id", UUID(Modifiers(nullable=True, low_cardinality=False))),
    # profiler_id is nullable since this will only be used by continuous profiling
    Column("profiler_id", UUID(Modifiers(nullable=True, low_cardinality=False))),
    # transaction_name is nullable since this will only be used by transaction-based profiling
    Column("transaction_name", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("fingerprint", UInt(64)),
    Column("name", String()),
    Column("package", String()),
    Column("thread_id", String()),
    Column("min", Float(64)),
    Column("max", Float(64)),
    Column("sum", Float(64)),
    Column("count", UInt(64)),
    Column("end_timestamp", DateTime()),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    # snuba internals
    Column("retention_days", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.MergeTree(
                    order_by="(project_id, end_timestamp)",
                    partition_by="(retention_days, toMonday(end_timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set_name,
                    ttl="end_timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key=None,
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
