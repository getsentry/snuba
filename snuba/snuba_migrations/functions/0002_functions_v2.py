from dataclasses import dataclass
from typing import List, Sequence

from snuba.clickhouse.columns import (
    UUID,
    AggregateFunction,
    Column,
    DateTime,
    Float,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


@dataclass(frozen=True)
class NewColumn:
    column: Column
    after: str


UNKNOWN_SPAN_STATUS = 2

new_columns: Sequence[NewColumn] = [
    NewColumn(column=Column("function", String()), after="name"),
    NewColumn(column=Column("module", String()), after="package"),
    NewColumn(
        column=Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
        after="release",
    ),
    NewColumn(
        column=Column("transaction_op", String(Modifiers(low_cardinality=True))),
        after="dist",
    ),
    NewColumn(
        column=Column(
            "transaction_status", UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS)))
        ),
        after="transaction_op",
    ),
    NewColumn(
        column=Column(
            "http_method", String(Modifiers(nullable=True, low_cardinality=True))
        ),
        after="transaction_status",
    ),
    NewColumn(
        column=Column(
            "browser_name", String(Modifiers(nullable=True, low_cardinality=True))
        ),
        after="http_method",
    ),
    NewColumn(
        column=Column("device_classification", UInt(8)),
        after="browser_name",
    ),
]

base_columns: List[Column] = [
    Column("project_id", UInt(64)),
    Column("transaction_name", String()),
    Column("timestamp", DateTime()),
    Column("fingerprint", UInt(64)),
    Column("function", String()),
    Column("package", String()),
    Column("module", String()),
    Column("is_application", UInt(8)),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("transaction_op", String(Modifiers(low_cardinality=True))),
    Column("transaction_status", UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS)))),
    Column("http_method", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("browser_name", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("device_classification", UInt(8)),
    Column("retention_days", UInt(16)),
]

materialized_columns: Sequence[Column] = base_columns + [
    Column("count", AggregateFunction("count", [Float(64)])),
    Column(
        "percentiles",
        AggregateFunction("quantiles(0.5, 0.75, 0.9, 0.95, 0.99)", [Float(64)]),
    ),
    Column("min", AggregateFunction("min", [Float(64)])),
    Column("max", AggregateFunction("max", [Float(64)])),
    Column("avg", AggregateFunction("avg", [Float(64)])),
    Column("sum", AggregateFunction("sum", [Float(64)])),
    Column("worst", AggregateFunction("argMax", [UUID(), Float(64)])),
    Column("examples", AggregateFunction("groupUniqArray(5)", [UUID()])),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set = StorageSetKey.FUNCTIONS

    materialization_version = 1

    index_granularity = "2048"

    data_granularity = 60 * 60  # 1 hour buckets

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    local_materialized_table = "functions_v2_local"
    dist_materialized_table = "functions_v2_dist"

    local_view_table = "functions_view_v2_local"

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        raw_table_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=new_column.column,
                after=new_column.after,
                target=target,
            )
            for table_name, target in [
                (self.local_raw_table, OperationTarget.LOCAL),
                (self.dist_raw_table, OperationTarget.DISTRIBUTED),
            ]
            for new_column in new_columns
        ]

        mat_table_ops: List[operations.SqlOperation] = [
            operations.CreateTable(
                storage_set=self.storage_set,
                table_name=table_name,
                columns=materialized_columns,
                engine=engine,
                target=target,
            )
            for table_name, engine, target in [
                (
                    self.local_materialized_table,
                    table_engines.AggregatingMergeTree(
                        storage_set=self.storage_set,
                        order_by=f"({', '.join(column.name for column in base_columns)})",
                        primary_key="(project_id, transaction_name, timestamp, fingerprint)",
                        partition_by="(retention_days, toMonday(timestamp))",
                        settings={"index_granularity": self.index_granularity},
                        ttl="timestamp + toIntervalDay(retention_days)",
                    ),
                    OperationTarget.LOCAL,
                ),
                (
                    self.dist_materialized_table,
                    table_engines.Distributed(
                        local_table_name=self.local_materialized_table,
                        sharding_key=None,
                    ),
                    OperationTarget.DISTRIBUTED,
                ),
            ]
        ]

        mat_view_ops: List[operations.SqlOperation] = [
            operations.CreateMaterializedView(
                storage_set=self.storage_set,
                view_name=self.local_view_table,
                destination_table_name=self.local_materialized_table,
                columns=materialized_columns,
                query=self.__MATVIEW_STATEMENT,
                target=OperationTarget.LOCAL,
            ),
        ]

        return raw_table_ops + mat_table_ops + mat_view_ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        raw_table_ops: List[operations.SqlOperation] = [
            operations.DropColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column_name=new_column.column.name,
                target=target,
            )
            for table_name, target in [
                (self.local_raw_table, OperationTarget.LOCAL),
                (self.dist_raw_table, OperationTarget.DISTRIBUTED),
            ]
            for new_column in new_columns
        ]

        mat_table_ops: List[operations.SqlOperation] = [
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=table_name,
                target=target,
            )
            for table_name, target in [
                (self.local_materialized_table, OperationTarget.LOCAL),
                (self.dist_materialized_table, OperationTarget.DISTRIBUTED),
            ]
        ]

        mat_view_ops = [
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=self.local_view_table,
                target=OperationTarget.LOCAL,
            ),
        ]

        return mat_view_ops + mat_table_ops + raw_table_ops

    @property
    def __MATVIEW_STATEMENT(self) -> str:
        return f"""
            SELECT
                project_id,
                transaction_name,
                toDateTime({self.data_granularity} * intDiv(toUnixTimestamp(timestamp), {self.data_granularity})) AS timestamp,
                fingerprint,
                function,
                package,
                module,
                is_application,
                platform,
                environment,
                release,
                dist,
                transaction_op,
                transaction_status,
                http_method,
                browser_name,
                device_classification,
                retention_days,
                countState(arrayJoin(durations) AS duration) AS count,
                quantilesState(0.5, 0.75, 0.9, 0.95, 0.99)(duration) AS percentiles,
                minState(duration) AS min,
                maxState(duration) AS max,
                avgState(duration) AS avg,
                sumState(duration) AS sum,
                argMaxState(profile_id, duration) as worst,
                groupUniqArrayState(5)(profile_id) as examples
            FROM {self.local_raw_table}
            WHERE materialization_version = {self.materialization_version}
            GROUP BY
                project_id,
                transaction_name,
                timestamp,
                fingerprint,
                function,
                package,
                module,
                is_application,
                platform,
                environment,
                release,
                dist,
                transaction_op,
                transaction_status,
                http_method,
                browser_name,
                device_classification,
                retention_days
        """
