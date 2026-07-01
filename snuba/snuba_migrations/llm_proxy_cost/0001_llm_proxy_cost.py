from collections.abc import Sequence

from snuba.clickhouse.columns import Column, DateTime, Float, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.LLM_PROXY_COST
table_prefix = "llm_proxy_cost_raw"
local_table_name = f"{table_prefix}_local"
dist_table_name = f"{table_prefix}_dist"

columns: list[Column[Modifiers]] = [
    Column("timestamp", DateTime()),
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("feature", String(Modifiers(low_cardinality=True))),
    Column("model", String(Modifiers(low_cardinality=True))),
    Column("region", String(Modifiers(low_cardinality=True))),
    Column("call_type", String(Modifiers(low_cardinality=True))),
    Column("prompt_tokens", UInt(32)),
    Column("completion_tokens", UInt(32)),
    Column("cache_read_tokens", UInt(32)),
    Column("cache_write_tokens", UInt(32)),
    Column("total_cost_usd", Float(64)),
    Column("input_cost_usd", Float(64)),
    Column("output_cost_usd", Float(64)),
    Column("cache_read_cost_usd", Float(64)),
    Column("cache_write_cost_usd", Float(64)),
    Column("litellm_cost_usd", Float(64)),
    Column("is_long_context", UInt(8)),
    Column("is_regional", UInt(8)),
    Column("response_time_ms", Float(64)),
    Column("litellm_call_id", String()),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=storage_set,
                    order_by="(org_id, project_id, timestamp)",
                    partition_by="(toStartOfMonth(timestamp))",
                    settings={"index_granularity": "8192"},
                    ttl="timestamp + toIntervalDay(90)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="org_id",
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
