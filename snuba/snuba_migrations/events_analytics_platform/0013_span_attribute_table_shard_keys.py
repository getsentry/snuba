from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import (
    UUID,
    Column,
    DateTime,
    Float,
    SimpleAggregateFunction,
    String,
    UInt,
)


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration creates a table meant to store just the attributes seen in a particular org.
    The table is populated by a separate materialized view for each type of attribute.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    num_view_name = "spans_num_attrs_mv"
    num_local_table_name = "spans_num_attrs_local"
    num_dist_table_name = "spans_num_attrs_dist"
    num_table_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column(
            "trace_id", UUID()
        ),  # recommended by altinity, this lets us find traces which have k=v set
        Column("project_id", UInt(64)),
        Column("attr_key", String()),
        Column("attr_value", Float(64)),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16)),
        Column("duration_ms", SimpleAggregateFunction("max", [UInt(32)])),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    str_view_name = "spans_str_attrs_mv"
    str_local_table_name = "spans_str_attrs_local"
    str_dist_table_name = "spans_str_attrs_dist"
    str_table_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column(
            "trace_id", UUID()
        ),  # recommended by altinity, this lets us find traces which have k=v set
        Column("attr_key", String()),
        Column("attr_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.num_local_table_name,
                    sharding_key="cityHash64(reinterpretAsUInt128(trace_id))",  # sharding keys must be at most 64 bits
                ),
                columns=self.num_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.str_local_table_name,
                    sharding_key="cityHash64(reinterpretAsUInt128(trace_id))",  # sharding keys must be at most 64 bits
                ),
                columns=self.str_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.num_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.num_local_table_name,
                    sharding_key=None,
                ),
                columns=self.num_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.str_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.str_local_table_name,
                    sharding_key=None,
                ),
                columns=self.str_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
