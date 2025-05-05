from typing import List, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation


def local_table_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_local"


def old_materialized_view_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_mv_2"


def old_materialized_view_expression(sampling_weight: int, columns: str) -> str:
    column_names_str = ", ".join(
        [
            f"{c.name}"
            for c in columns
            if c.name != "sampling_weight" and c.name != "sampling_factor"
        ]
    )
    return (
        f"SELECT {column_names_str}, "
        f"sampling_weight * {sampling_weight} AS sampling_weight, "
        f"sampling_factor / {sampling_weight} AS sampling_factor "
        "FROM eap_items_1_local "
        f"WHERE (cityHash64(item_id + {sampling_weight})  % {sampling_weight}) = 0"
    )


def new_materialized_view_name(weight: int) -> str:
    return f"eap_items_1_downsample_{weight}_mv_3"


def new_materialized_view_expression(sampling_weight: int, columns: str) -> str:
    column_names_str = ", ".join(
        [
            f"{c.name}"
            for c in columns
            if c.name != "sampling_weight" and c.name != "sampling_factor"
        ]
    )
    return (
        f"SELECT {column_names_str}, "
        f"sampling_weight * {sampling_weight} AS sampling_weight, "
        f"sampling_factor / {sampling_weight} AS sampling_factor "
        "FROM eap_items_1_local "
        f"WHERE (cityHash64(trace_id)  % {sampling_weight}) = 0"
    )


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    granularity = "8192"

    storage_set_key = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
    storage = get_storage(StorageKey.EAP_ITEMS)

    sampling_weights = [8, 8**2, 8**3]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []
        columns: ColumnSet = self.storage.get_schema().get_columns()

        for sampling_weight in self.sampling_weights:
            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=self.storage_set_key,
                        view_name=new_materialized_view_name(sampling_weight),
                        columns=columns,
                        destination_table_name=local_table_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                        query=new_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=old_materialized_view_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops

    def backwards_ops(self) -> Sequence[SqlOperation]:
        ops: List[SqlOperation] = []
        columns: ColumnSet = self.storage.get_schema().get_columns()

        for sampling_weight in self.sampling_weights:
            local_table_name = f"eap_items_1_downsample_{sampling_weight}_local"
            ops.extend(
                [
                    operations.CreateMaterializedView(
                        storage_set=self.storage_set_key,
                        view_name=old_materialized_view_name(sampling_weight),
                        columns=columns,
                        destination_table_name=local_table_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                        query=old_materialized_view_expression(sampling_weight),
                    ),
                    operations.DropTable(
                        storage_set=self.storage_set_key,
                        table_name=new_materialized_view_name(sampling_weight),
                        target=OperationTarget.LOCAL,
                    ),
                ]
            )

        return ops
