from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import AddIndicesData, SqlOperation

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_spans_local"

indices: Sequence[AddIndicesData] = [
    AddIndicesData(
        name="bf_attr_str_0",
        expression="mapKeys(attr_str_0)",
        type="bloom_filter",
        granularity=1,
    ),
    AddIndicesData(
        name="bf_attr_num_0",
        expression="mapKeys(attr_num_0)",
        type="bloom_filter",
        granularity=1,
    ),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                index_name=idx.name,
                target=operations.OperationTarget.LOCAL,
            )
            for idx in indices
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return []
