from __future__ import annotations

from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
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
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

    str_mv = "spans_str_attrs_2_mv"
    str_local_table = "spans_str_attrs_2_local"
    str_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("attr_value", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    num_mv = "spans_num_attrs_2_mv"
    num_local_table = "spans_num_attrs_2_local"
    num_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("attr_key", String()),
        Column("attr_min_value", SimpleAggregateFunction("min", [Float(64)])),
        Column("attr_max_value", SimpleAggregateFunction("max", [Float(64)])),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
        Column("retention_days", UInt(16)),
        Column("count", SimpleAggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.str_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.str_mv,
                columns=self.str_columns,
                destination_table_name=self.str_local_table,
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
FROM eap_spans_local
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
            operations.CreateMaterializedView(
                storage_set=self.storage_set_key,
                view_name=self.num_mv,
                columns=self.num_columns,
                destination_table_name=self.num_local_table,
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
FROM eap_spans_local
LEFT ARRAY JOIN
    arrayConcat(
        {",".join(f"CAST(attr_num_{n}, 'Array(Tuple(String, Float64))')" for n in range(ATTRIBUTE_BUCKETS))},
        array(
            tuple('sentry.duration_ms', `duration_ms`::Float64)
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
                table_name=self.str_mv,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.num_mv,
                target=OperationTarget.LOCAL,
            ),
        ]
