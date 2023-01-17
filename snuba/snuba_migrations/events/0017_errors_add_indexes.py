from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds indexes and settings to the errors table to match SaaS.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="bf_tags_hash_map",
                index_expression="_tags_hash_map",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="minmax_group_id",
                index_expression="group_id",
                index_type="minmax",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="bf_release",
                index_expression="release",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.ModifyTableSettings(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                settings={
                    "min_bytes_for_wide_part": 1,
                    "enable_vertical_merge_algorithm": 1,
                    "min_rows_for_wide_part": 0,
                    "ttl_only_drop_parts": 1,
                },
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="bf_tags_hash_map",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="minmax_group_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                index_name="bf_release",
                target=operations.OperationTarget.LOCAL,
            ),
            # Ideally we would call ResetTableSetting which would do ALTER TABLE .. RESET SETTING,
            # but that is not supported in Clickhouse 20
            operations.ModifyTableSettings(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                settings={
                    "min_bytes_for_wide_part": "DEFAULT",
                    "enable_vertical_merge_algorithm": "DEFAULT",
                    "min_rows_for_wide_part": "DEFAULT",
                    "ttl_only_drop_parts": "DEFAULT",
                },
                target=operations.OperationTarget.LOCAL,
            ),
        ]
