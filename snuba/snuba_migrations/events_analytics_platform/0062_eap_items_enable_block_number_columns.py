from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM

# Mirrors the sampling weights used by the eap_items downsample tables.
sampling_weights = [8, 8**2, 8**3]


def _local_tables() -> list[str]:
    prefixes = ["eap_items_1"] + [
        f"eap_items_1_downsample_{sampling_weight}" for sampling_weight in sampling_weights
    ]
    return [f"{prefix}_local" for prefix in prefixes]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        # enable_block_number_column / enable_block_offset_column are MergeTree
        # per-table settings, so they only apply to the local tables.
        return [
            operations.ModifyTableSettings(
                storage_set=storage_set,
                table_name=table_name,
                settings={
                    "enable_block_number_column": 1,
                    "enable_block_offset_column": 1,
                },
                target=OperationTarget.LOCAL,
            )
            for table_name in _local_tables()
        ]

    def backwards_ops(self) -> list[SqlOperation]:
        return [
            operations.ModifyTableSettings(
                storage_set=storage_set,
                table_name=table_name,
                settings={
                    "enable_block_number_column": 0,
                    "enable_block_offset_column": 0,
                },
                target=OperationTarget.LOCAL,
            )
            for table_name in _local_tables()
        ]
