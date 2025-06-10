from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


def hash_map_column_name(attribute_type: str, i: int) -> str:
    return f"_hash_map_{attribute_type}_{i}"


TABLES = [
    ("dist", OperationTarget.DISTRIBUTED),
    ("local", OperationTarget.LOCAL),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    downsampled_factors = [8, 64, 512]
    buckets = range(1)
    table_name_prefix = "eap_items_1"

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return []

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return []
