from typing import List, MutableMapping, Optional, Sequence, Union

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
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, migration_utilities, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("fingerprint", UInt(64)),
    Column("function", String()),
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
                    UUID(),  # profile_id
                    String(),  # thread_id
                    DateTime64(
                        precision=6,
                        modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
                    ),  # start_timestamp
                    DateTime64(
                        precision=6,
                        modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
                    ),  # end_timestamp
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
                    UUID(),  # profile_id
                    String(),  # thread_id
                    DateTime64(
                        precision=6,
                        modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
                    ),  # start_timestamp
                    DateTime64(
                        precision=6,
                        modifiers=Modifiers(nullable=True, codecs=["DoubleDelta"]),
                    ),  # end_timestamp
                ),  # end Tuple
            ],
        ),
    ),
    Column("retention_days", UInt(16)),
]


class Migration(migration.CodeMigration):
    blocking = False
    index_granularity = "2048"
    storage_set = StorageSetKey.FUNCTIONS

    data_granularity = 60 * 60  # 1 hour buckets

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    local_functions_table = "functions_v2_local"
    dist_functions_table = "functions_v2_dist"

    local_mv_table = "functions_mv_v2_local"

    def _create_functions_v2_table(
        self, clickhouse: Optional[ClickhousePool]
    ) -> operations.SqlOperation:
        table_settings: MutableMapping[str, Union[int, str]] = {
            "index_granularity": self.index_granularity,
        }

        clickhouse_version = migration_utilities.get_clickhouse_version_for_storage_set(
            self.storage_set, clickhouse
        )
        if migration_utilities.supports_setting(
            clickhouse_version, "allow_nullable_key"
        ):
            table_settings["allow_nullable_key"] = 1

        return operations.CreateTable(
            storage_set=self.storage_set,
            table_name=self.local_functions_table,
            columns=columns,
            engine=table_engines.AggregatingMergeTree(
                storage_set=self.storage_set,
                order_by="(project_id, timestamp, transaction_name, fingerprint, function, package, is_application, profiling_type, platform, environment, release, retention_days)",
                primary_key="(project_id, timestamp, transaction_name, fingerprint)",
                partition_by="(retention_days, toMonday(timestamp))",
                settings=table_settings,
                ttl="timestamp + toIntervalDay(retention_days)",
            ),
            target=operations.OperationTarget.LOCAL,
        )

    def forwards_global(self) -> Sequence[operations.GenericOperation]:
        return [*self._forwards_local(), *self._forwards_dist()]

    def backwards_global(self) -> Sequence[operations.GenericOperation]:
        return [*self._backwards_dist(), *self._backwards_local()]

    def _forwards_local(self) -> Sequence[operations.GenericOperation]:
        return [
            operations.RunSqlAsCode(self._create_functions_v2_table),
            operations.RunSqlAsCode(
                operations.CreateMaterializedView(
                    storage_set=self.storage_set,
                    view_name=self.local_mv_table,
                    destination_table_name=self.local_functions_table,
                    columns=columns,
                    query=self.__MATVIEW_STATEMENT,
                    target=operations.OperationTarget.LOCAL,
                )
            ),
        ]

    def _backwards_local(self) -> Sequence[operations.GenericOperation]:
        return [
            operations.RunSqlAsCode(
                operations.DropTable(
                    storage_set=self.storage_set,
                    table_name=self.local_functions_table,
                    target=operations.OperationTarget.LOCAL,
                )
            ),
            operations.RunSqlAsCode(
                operations.DropTable(
                    storage_set=self.storage_set,
                    table_name=self.local_mv_table,
                    target=operations.OperationTarget.LOCAL,
                )
            ),
        ]

    def _forwards_dist(self) -> Sequence[operations.GenericOperation]:
        return [
            operations.RunSqlAsCode(
                operations.CreateTable(
                    storage_set=self.storage_set,
                    table_name=self.dist_functions_table,
                    columns=columns,
                    engine=table_engines.Distributed(
                        local_table_name=self.dist_functions_table,
                        sharding_key=None,
                    ),
                    target=operations.OperationTarget.DISTRIBUTED,
                )
            ),
        ]

    def _backwards_dist(self) -> Sequence[operations.GenericOperation]:
        return [
            operations.RunSqlAsCode(
                operations.DropTable(
                    storage_set=self.storage_set,
                    table_name=self.dist_functions_table,
                    target=operations.OperationTarget.DISTRIBUTED,
                )
            ),
        ]

    @property
    def __MATVIEW_STATEMENT(self) -> str:
        return f"""
            SELECT
                project_id,
                transaction_name,
                function,
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
                function,
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
