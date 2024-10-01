from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Float

storage_set_name = StorageSetKey.FUNCTIONS_EXAMPLES
local_table_name = "functions_examples_local"
dist_table_name = "functions_examples_dist"

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("profile_id", UUID(Modifiers(low_cardinality=False))),
    Column("profiling_type", String(Modifiers(low_cardinality=True))),
    Column("transaction_name", String()),
    Column("fingerprint", UInt(64)),
    Column("name", String()),
    Column("package", String()),
    Column("thread_id", String()),
    Column("min", Float(64)),
    Column("max", Float(64)),
    Column("sum", Float(64)),
    Column("count", UInt(64)),
    Column("start_timestamp", DateTime()),
    Column("end_timestamp", DateTime()),
    Column("is_application", UInt(8)),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True))),
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
                    order_by="(project_id, start_timestamp, fingerprint, transaction_name)",
                    partition_by="(retention_days, toMonday(start_timestamp))",
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
