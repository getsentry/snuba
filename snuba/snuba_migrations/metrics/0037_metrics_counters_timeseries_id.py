from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Adds the timeseries_id column to the metrics counters table so we can add
    a sharding key for each timeseries and scale the release health cluster.
    """

    blocking = False

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=table_name,
                column=Column("timeseries_id", UInt(32)),
            ),
        ]

    def __backward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=table_name,
                column_name="timeseries_id",
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("metrics_counters_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("metrics_counters_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("metrics_counters_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("metrics_counters_dist")
