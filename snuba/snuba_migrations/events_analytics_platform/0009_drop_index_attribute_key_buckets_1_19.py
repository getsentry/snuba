from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndices(
                storage_set=StorageSetKey.EVENTS_ANALYTICS_PLATFORM,
                table_name="eap_spans_local",
                indices=[
                    index_name
                    for bucket in range(1, 20)
                    for index_name in {f"bf_attr_num_{bucket}", f"bf_attr_str_{bucket}"}
                ],
                target=operations.OperationTarget.LOCAL,
                run_async=True,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return []
