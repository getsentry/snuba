from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Column,
    DateTime,
    DateTime64,
    Float,
    String,
    Tuple,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("fingerprint", UInt(64)),
    Column("name", String()),
    Column("package", String()),
    Column("is_application", UInt(8)),
    Column("profiling_type", String(Modifiers(low_cardinality=True))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    # aggregate columns
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
        "worst",
        AggregateFunction(
            "argMax",
            [
                Tuple(
                    (
                        UUID(),  # profile_id
                        String(),  # thread_id
                        DateTime64(  # start_timestamp
                            precision=6,
                            modifiers=Modifiers(nullable=True),
                        ),
                        DateTime64(  # end_timestamp
                            precision=6,
                            modifiers=Modifiers(nullable=True),
                        ),
                    )
                ),  # end Tuple
                Float(64),
            ],
        ),
    ),
    Column(
        "examples",
        AggregateFunction(
            "groupUniqArray(5)",
            [
                Tuple(
                    (
                        UUID(),  # profile_id
                        String(),  # thread_id
                        DateTime64(
                            precision=6,
                            modifiers=Modifiers(nullable=True),
                        ),  # start_timestamp
                        DateTime64(
                            precision=6,
                            modifiers=Modifiers(nullable=True),
                        ),  # end_timestamp
                    )
                ),  # end Tuple
            ],
        ),
    ),
    Column("retention_days", UInt(16)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    storage_set = StorageSetKey.FUNCTIONS

    data_granularity = 60 * 60  # 1 hour buckets

    local_raw_table = "functions_raw_local"

    local_functions_table = "functions_v2_local"
    dist_functions_table = "functions_v2_dist"

    local_mv_table = "functions_mv_v2_local"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.local_functions_table,
                columns=columns,
                engine=table_engines.AggregatingMergeTree(
                    storage_set=self.storage_set,
                    order_by="(project_id, timestamp, transaction_name, fingerprint, name, package, is_application, profiling_type, platform, environment, release, retention_days)",
                    primary_key="(project_id, timestamp, transaction_name, fingerprint)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "2048", "allow_nullable_key": 1},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateMaterializedView(
                storage_set=self.storage_set,
                view_name=self.local_mv_table,
                destination_table_name=self.local_functions_table,
                columns=columns,
                query=self.__MATVIEW_STATEMENT,
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=self.dist_functions_table,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=self.dist_functions_table,
                    sharding_key=None,
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=self.dist_functions_table,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=self.local_functions_table,
                target=OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=self.local_mv_table,
                target=OperationTarget.LOCAL,
            ),
        ]

    @property
    def __MATVIEW_STATEMENT(self) -> str:
        return f"""
            SELECT
                project_id,
                transaction_name,
                name,
                package,
                fingerprint,
                is_application,
                profiling_type,
                platform,
                environment,
                release,
                toDateTime({self.data_granularity} * intDiv(toUnixTimestamp(timestamp), {self.data_granularity})) AS timestamp,
                retention_days,
                countState(arrayJoin(durations) AS duration) AS count,
                quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS percentiles,
                minState(duration) AS min,
                maxState(duration) AS max,
                avgState(duration) AS avg,
                sumState(duration) AS sum,
                argMaxState((profile_id, thread_id, start_timestamp, end_timestamp), duration) as worst,
                groupUniqArrayState(5)((profile_id, thread_id, start_timestamp, end_timestamp)) as examples
            FROM {self.local_raw_table}
            WHERE materialization_version = 0
            GROUP BY
                project_id,
                transaction_name,
                name,
                package,
                fingerprint,
                is_application,
                profiling_type
                platform,
                environment,
                release,
                timestamp,
                retention_days
        """
