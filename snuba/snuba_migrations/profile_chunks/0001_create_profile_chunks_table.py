from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime64, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.PROFILE_CHUNKS
table_prefix = "profile_chunks"
local_table_name = f"{table_prefix}_local"
dist_table_name = f"{table_prefix}_dist"

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("profiler_id", UUID()),
    Column("chunk_id", UUID()),
    Column(
        "start_timestamp",
        DateTime64(
            precision=6,
            modifiers=Modifiers(codecs=["DoubleDelta"]),
        ),
    ),
    Column(
        "end_timestamp",
        DateTime64(
            precision=6,
            modifiers=Modifiers(codecs=["DoubleDelta"]),
        ),
    ),
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    order_by="(project_id, profiler_id, start_timestamp, cityHash64(chunk_id))",
                    partition_by="(retention_days, toStartOfDay(start_timestamp))",
                    sample_by="cityHash64(chunk_id)",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set,
                    ttl="toDateTime(end_timestamp) + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="cityHash64(profiler_id)",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=storage_set,
                table_name=local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
