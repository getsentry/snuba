from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Float

storage_set_name = StorageSetKey.METRICS_SUMMARIES
local_table_name = "metrics_summaries_local"
dist_table_name = "metrics_summaries_dist"

columns: List[Column[Modifiers]] = [
    # ids
    Column("project_id", UInt(64)),
    Column("span_id", UInt(64)),
    Column("trace_id", UUID()),
    # metrics summary
    Column("metric_mri", String(Modifiers(low_cardinality=True))),
    Column("min", Float(64)),
    Column("max", Float(64)),
    Column("sum", Float(64)),
    Column("count", UInt(64)),
    # metrics metadata
    Column("tags", Nested([("key", String()), ("value", String())])),
    # span metadata
    Column("end_timestamp", DateTime()),
    # snuba internals
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set_name,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(project_id, metric_mri, end_timestamp, cityHash64(span_id))",
                    version_column="deleted",
                    partition_by="(retention_days, toMonday(end_timestamp))",
                    sample_by="metric_mri",
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
                    sharding_key="cityHash64(trace_id)",
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
