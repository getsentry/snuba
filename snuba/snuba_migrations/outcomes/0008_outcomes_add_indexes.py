from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds indexes and codecs to match schema in SaaS. Clickhouse 20 doesn't
    support adding codecs to key columns, so we don't add codecs to timestamp.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_key_id",
                index_expression="key_id",
                index_type="minmax",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_outcome",
                index_expression="outcome",
                index_type="minmax",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_key_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                index_name="minmax_outcome",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
