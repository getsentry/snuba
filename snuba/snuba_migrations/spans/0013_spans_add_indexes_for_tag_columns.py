from typing import List, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations

table_name_prefix = "spans"
storage_set = StorageSetKey.SPANS
indexes = [
    ("bf_tags_key", "tags.key", "bloom_filter(0.0)", 1),
    ("bf_tags_hash_map", "_tags_hash_map", "bloom_filter(0.0)", 1),
    ("bf_sentry_tags_hash_map", "_sentry_tags_hash_map", "bloom_filter(0.0)", 1),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        for index in indexes:
            index_name, index_expression, index_type, granularity = index
            ops.append(
                operations.AddIndex(
                    storage_set=storage_set,
                    table_name=f"{table_name_prefix}_local",
                    index_name=index_name,
                    index_expression=index_expression,
                    index_type=index_type,
                    granularity=granularity,
                    target=operations.OperationTarget.LOCAL,
                )
            )
        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops: List[operations.SqlOperation] = []
        for index in indexes:
            index_name, _, _, _ = index
            ops.append(
                operations.DropIndex(
                    storage_set=storage_set,
                    table_name=f"{table_name_prefix}_local",
                    index_name=index_name,
                    target=operations.OperationTarget.LOCAL,
                )
            )
        return ops
