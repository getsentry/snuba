from copy import deepcopy
from dataclasses import replace
from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import Materialized
from snuba.migrations.columns import MigrationModifiers as Modifiers

UNKNOWN_SPAN_STATUS = 2

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("event_id", UUID()),
    Column("trace_id", UUID()),
    Column("span_id", UInt(64)),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column(
        "transaction_hash",
        UInt(64, Modifiers(materialized="cityHash64(transaction_name)")),
    ),
    Column("transaction_op", String(Modifiers(low_cardinality=True))),
    Column("transaction_status", UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS)))),
    Column("start_ts", DateTime()),
    Column("start_ms", UInt(16)),
    Column("finish_ts", DateTime()),
    Column("finish_ms", UInt(16)),
    Column("duration", UInt(32)),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    Column("user", String(Modifiers(default="''"))),
    Column("user_hash", UInt(64, Modifiers(materialized="cityHash64(user)"))),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(low_cardinality=True, default="''"))),
    Column("sdk_version", String(Modifiers(low_cardinality=True, default="''"))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("_contexts_flattened", String()),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.TRANSACTIONS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))",
                    partition_by="(retention_days, toMonday(finish_ts))",
                    sample_by="cityHash64(span_id)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="transactions_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        # We removed the materialized for the dist table DDL.
        def strip_materialized(columns: Sequence[Column]) -> None:
            for col in columns:
                if col.type.has_modifier(Materialized):
                    modifiers = col.type.get_modifiers()
                    assert modifiers is not None
                    col.type = col.type.set_modifiers(
                        replace(modifiers, materialized=None)
                    )

        dist_columns = deepcopy(columns)
        strip_materialized(dist_columns)

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                columns=dist_columns,
                engine=table_engines.Distributed(
                    local_table_name="transactions_local",
                    sharding_key="cityHash64(span_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="transactions_dist",
            )
        ]
