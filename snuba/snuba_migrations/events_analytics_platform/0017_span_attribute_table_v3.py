from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ATTRIBUTE_BUCKETS
from snuba.utils.schemas import (
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

    old_str_mv = "spans_str_attrs_2_mv"
    old_str_local_table = "spans_str_attrs_2_local"
    old_str_dist_table = "spans_str_attrs_2_dist"
    new_str_mv = "spans_str_attrs_3_mv"
    new_str_local_table = "spans_str_attrs_3_local"
    new_str_dist_table = "spans_str_attrs_3_dist"
    str_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_value", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column(
            "timestamp",
            DateTime(modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
        ),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    old_num_mv = "spans_num_attrs_2_mv"
    old_num_local_table = "spans_num_attrs_2_local"
    old_num_dist_table = "spans_num_attrs_2_dist"
    new_num_mv = "spans_num_attrs_3_mv"
    new_num_local_table = "spans_num_attrs_3_local"
    new_num_dist_table = "spans_num_attrs_3_dist"
    num_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_min_value", SimpleAggregateFunction("min", [Float(64)])),
        Column("attr_max_value", SimpleAggregateFunction("max", [Float(64)])),
        Column(
            "timestamp",
            DateTime(modifiers=Modifiers(codecs=["DoubleDelta", "ZSTD(1)"])),
        ),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.new_str_local_table,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attr_key)",
                    order_by="(organization_id, attr_key, attr_value, project_id, timestamp, retention_days)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={
                        "index_granularity": self.granularity,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.str_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.new_str_dist_table,
                engine=table_engines.Distributed(
                    local_table_name=self.new_str_local_table, sharding_key=None
                ),
                columns=self.str_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.new_str_mv,
                columns=self.str_columns,
                destination_table_name=self.new_str_local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    attrs.1 as attr_key,
    attrs.2 as attr_value,
    toStartOfDay(_sort_timestamp) AS timestamp,
    retention_days,
    1 AS count
FROM eap_spans_2_local
LEFT ARRAY JOIN
    arrayConcat(
        {", ".join(f"CAST(attr_str_{n}, 'Array(Tuple(String, String))')" for n in range(ATTRIBUTE_BUCKETS))},
        array(
            tuple('sentry.service', `service`),
            tuple('sentry.segment_name', `segment_name`),
            tuple('sentry.name', `name`)
        )
    ) AS attrs
GROUP BY
    organization_id,
    project_id,
    attr_key,
    attr_value,
    timestamp,
    retention_days
""",
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.new_num_local_table,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attr_key)",
                    order_by="(organization_id, attr_key, timestamp, project_id, retention_days)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={
                        "index_granularity": self.granularity,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.num_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.new_num_dist_table,
                engine=table_engines.Distributed(
                    local_table_name=self.new_num_local_table, sharding_key=None
                ),
                columns=self.num_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.new_num_mv,
                columns=self.num_columns,
                destination_table_name=self.new_num_local_table,
                target=OperationTarget.LOCAL,
                query=f"""
SELECT
    organization_id,
    project_id,
    attrs.1 as attr_key,
    attrs.2 as attr_min_value,
    attrs.2 as attr_max_value,
    toStartOfDay(_sort_timestamp) AS timestamp,
    retention_days,
    1 AS count
FROM eap_spans_2_local
LEFT ARRAY JOIN
    arrayConcat(
        {",".join(f"CAST(attr_num_{n}, 'Array(Tuple(String, Float64))')" for n in range(ATTRIBUTE_BUCKETS))},
        array(
            tuple('sentry.duration_ms', duration_micro / 1000)
        )
    ) AS attrs
GROUP BY
    organization_id,
    project_id,
    attrs.1,
    attrs.2,
    timestamp,
    retention_days
""",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_str_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_str_local_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_str_dist_table,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_num_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_num_local_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.new_num_dist_table,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
