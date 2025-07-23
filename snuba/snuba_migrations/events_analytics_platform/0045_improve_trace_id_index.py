from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name = "eap_items_1_local"
index_name = "bf_trace_id"
index_expression = "trace_id"
current_index_type = "bloom_filter"
new_index_type = "bloom_filter(0.0001)"
granularity = 1
target = OperationTarget.LOCAL


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=storage_set,
                table_name=table_name,
                index_name=index_name,
                target=target,
            ),
            operations.AddIndex(
                storage_set=storage_set,
                table_name=table_name,
                index_name=index_name,
                index_expression=index_expression,
                index_type=new_index_type,
                granularity=granularity,
                target=target,
            ),
        ]

    def backwards_ops(self) -> list[SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=storage_set,
                table_name=table_name,
                index_name=index_name,
                target=target,
            ),
            operations.AddIndex(
                storage_set=storage_set,
                table_name=table_name,
                index_name=index_name,
                index_expression=index_expression,
                index_type=current_index_type,
                granularity=granularity,
                target=target,
            ),
        ]
