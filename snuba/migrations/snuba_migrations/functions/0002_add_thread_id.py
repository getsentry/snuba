from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Column,
    DateTime,
    Float,
    String,
    Tuple,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("depth", UInt(32)),
    Column("parent_fingerprint", UInt(64)),
    Column("fingerprint", UInt(64)),
    Column("name", String()),
    Column("package", String()),
    Column("path", String()),
    Column("is_application", UInt(8)),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("os_name", String(Modifiers(low_cardinality=True))),
    Column("os_version", String(Modifiers(low_cardinality=True))),
    Column("retention_days", UInt(16)),
    Column("count", AggregateFunction("count", [Float(64)])),
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column(
        "worst", AggregateFunction("argMax", [Tuple((UUID(), UInt(64))), Float(64)])
    ),
    Column(
        "examples", AggregateFunction("groupUniqArray(5)", [Tuple((UUID(), UInt(64)))])
    ),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    index_granularity = "2048"
    storage_set = StorageSetKey.FUNCTIONS

    data_granularity = 60 * 60  # 1 hour buckets

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    local_materialized_table = "functions_mv_local"
    dist_materialized_table = "functions_mv_dist"

    local_view_table = "functions_local"

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=self.local_raw_table,
                column=Column("thread_id", UInt(64)),
                after="profile_id",
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.local_materialized_table
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.local_view_table
            ),
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.local_materialized_table,
                columns=columns,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set,
                    order_by="(project_id, transaction_name, timestamp, depth, parent_fingerprint, fingerprint, name, package, path, is_application, platform, environment, release, os_name, os_version, retention_days)",
                    primary_key="(project_id, transaction_name, timestamp)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": self.index_granularity},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set,
                view_name=self.local_view_table,
                destination_table_name=self.local_materialized_table,
                columns=columns,
                query=self.__MATVIEW_STATEMENT,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        # intentionally empty as this is a destructive migration and we do not want to go back
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.FUNCTIONS,
                table_name="functions_raw_dist",
                column=Column("thread_id", UInt(64)),
                after="profile_id",
            ),
            operations.DropTable(
                storage_set=self.storage_set, table_name=self.dist_materialized_table
            ),
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.dist_materialized_table,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=self.local_materialized_table,
                    sharding_key=None,
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        # intentionally empty as this is a destructive migration and we do not want to go back
        return []

    @property
    def __MATVIEW_STATEMENT(self) -> str:
        return f"""
            SELECT
                project_id,
                transaction_name,
                name,
                package,
                path,
                fingerprint,
                parent_fingerprint,
                depth,
                is_application,
                platform,
                environment,
                release,
                os_name,
                os_version,
                toDateTime({self.data_granularity} * intDiv(toUnixTimestamp(timestamp), {self.data_granularity})) AS timestamp,
                retention_days,
                countState(arrayJoin(durations) AS duration) AS count,
                quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS percentiles,
                minState(duration) AS min,
                maxState(duration) AS max,
                avgState(duration) AS avg,
                sumState(duration) AS sum,
                argMaxState((profile_id, thread_id), duration) as worst,
                groupUniqArrayState(5)((profile_id, thread_id)) as examples
            FROM {self.local_raw_table}
            WHERE materialization_version = 1
            GROUP BY
                project_id,
                transaction_name,
                name,
                package,
                path,
                fingerprint,
                parent_fingerprint,
                depth,
                is_application,
                platform,
                environment,
                release,
                os_name,
                os_version,
                timestamp,
                retention_days
        """
