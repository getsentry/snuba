from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import Float

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"

UNKNOWN_SPAN_STATUS = 2

columns: List[Column[Modifiers]] = [
    # ids
    Column("project_id", UInt(64)),
    Column("trace_id", UUID()),
    Column("span_id", UInt(64)),
    Column("segment_id", UInt(64)),
    # metrics summary
    Column("min", Float(64)),
    Column("max", Float(64)),
    Column("sum", Float(64)),
    Column("count", Float(64)),
    # metrics metadata
    Column("tags", Nested([("key", String()), ("value", String())])),
    # span metadata
    Column("start_timestamp", DateTime()),
    Column("exclusive_time", Float(64)),
    Column("op", String(Modifiers(low_cardinality=True))),
    Column("group", UInt(64)),
    # snuba internals
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
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
                    order_by="(project_id, group, cityHash64(span_id))",
                    version_column="deleted",
                    partition_by="(retention_days, toMonday(start_timestamp))",
                    sample_by="cityHash64(span_id)",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set_name,
                    ttl="start_timestamp + toIntervalDay(retention_days)",
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
