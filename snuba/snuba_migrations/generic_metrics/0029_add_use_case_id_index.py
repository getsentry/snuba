from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_COUNTERS,
                table_name="generic_metric_counters_aggregated_local",
                index_name="set_idx_use_case_id",
                index_expression="use_case_id",
                index_type="set(0)",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name="generic_metric_sets_local",
                index_name="set_idx_use_case_id",
                index_expression="use_case_id",
                index_type="set(0)",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
                table_name="generic_metric_distributions_aggregated_local",
                index_name="set_idx_use_case_id",
                index_expression="use_case_id",
                index_type="set(0)",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_GAUGES,
                table_name="generic_metric_gauges_aggregated_local",
                index_name="set_idx_use_case_id",
                index_expression="use_case_id",
                index_type="set(0)",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_GAUGES,
                table_name="generic_metric_gauges_aggregated_local",
                index_name="set_idx_use_case_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
                table_name="generic_metric_distributions_aggregated_local",
                index_name="set_idx_use_case_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_COUNTERS,
                table_name="generic_metric_counters_aggregated_local",
                index_name="set_idx_use_case_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name="generic_metric_sets_local",
                index_name="set_idx_use_case_id",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
