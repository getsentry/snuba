from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import SqlOperation

storage_set_name = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_spans_2_local"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=local_table_name,
                index_name="bf_project_id",
                index_expression="project_id",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name=local_table_name,
                index_name="bf_project_id",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
