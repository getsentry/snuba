from __future__ import annotations

from typing import Sequence

from snuba.clickhouse.columns import AggregateFunction, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.constants import ATTRIBUTE_BUCKETS

META_KEY_QUERY_TEMPLATE = """
SELECT
    organization_id,
    attribute_key,
    {attribute_value} AS attribute_value,
    toMonday(start_timestamp) AS timestamp,
    retention_days,
    sumState(cast(1, 'UInt64')) AS count
FROM eap_spans_local
LEFT ARRAY JOIN
    arrayConcat({key_columns}) AS attribute_key,
    arrayConcat({value_columns}) AS attr_value
GROUP BY
    organization_id,
    attribute_key,
    attribute_value,
    timestamp,
    retention_days
"""


class Migration(migration.ClickhouseNodeMigration):
    """
    This migration creates a table meant to store just the attributes seen in a particular org.
    The table is populated by a separate materialized view for each type of attribute.
    """

    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    granularity = "8192"

    value_types = ["str", "num"]

    meta_view_name = "spans_attributes_{attribute_type}_meta_mv"
    meta_local_table_name = "spans_attributes_meta_local"
    meta_dist_table_name = "spans_attributes_meta_dist"
    meta_table_columns: Sequence[Column[Modifiers]] = [
        Column("organization_id", UInt(64)),
        Column("attribute_type", String()),
        Column("attribute_key", String()),
        Column("attribute_value", String()),
        Column("timestamp", DateTime(modifiers=Modifiers(codecs=["DoubleDelta"]))),
        Column("retention_days", UInt(16)),
        Column("count", AggregateFunction("sum", [UInt(64)])),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        create_table_ops: list[SqlOperation] = [
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set_key,
                    primary_key="(organization_id, attribute_key)",
                    order_by="(organization_id, attribute_key, attribute_value, timestamp)",
                    partition_by="toMonday(timestamp)",
                    settings={
                        "index_granularity": self.granularity,
                        # Since the partitions contain multiple retention periods, need to ensure
                        # that rows within partitions are dropped
                        "ttl_only_drop_parts": 0,
                    },
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.meta_local_table_name, sharding_key=None
                ),
                columns=self.meta_table_columns,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

        materialized_view_ops: list[SqlOperation] = []
        for value_type in self.value_types:
            attribute_value = "attr_value" if value_type == "str" else "''"

            key_columns = ",".join(
                [f"mapKeys(attr_{value_type}_{i})" for i in range(ATTRIBUTE_BUCKETS)]
            )
            value_columns = ",".join(
                [f"mapValues(attr_{value_type}_{i})" for i in range(ATTRIBUTE_BUCKETS)]
            )

            materialized_view_ops.append(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set_key,
                    view_name=self.meta_view_name.format(attribute_type=value_type),
                    columns=self.meta_table_columns,
                    destination_table_name=self.meta_local_table_name,
                    target=OperationTarget.LOCAL,
                    query=META_KEY_QUERY_TEMPLATE.format(
                        attribute_value=attribute_value,
                        key_columns=key_columns,
                        value_columns=value_columns,
                    ),
                ),
            )

        return create_table_ops + materialized_view_ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: Sequence[SqlOperation] = [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_view_name.format(attribute_type=value_type),
                target=OperationTarget.LOCAL,
            )
            for value_type in self.value_types
        ] + [
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_local_table_name,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set_key,
                table_name=self.meta_dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]
        return ops
