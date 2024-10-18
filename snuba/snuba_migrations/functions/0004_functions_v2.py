from dataclasses import dataclass
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
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


@dataclass(frozen=True)
class NewColumn:
    column: Column[Modifiers]
    after: str | None


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
    Column("worst", AggregateFunction("argMax", [UUID(), Float(64)])),
    Column("examples", AggregateFunction("groupUniqArray(5)", [UUID()])),
]


new_columns: Sequence[NewColumn] = [
    NewColumn(
        Column(
            "worst_v2",
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
        after="worst",
    ),  # end new column
    NewColumn(
        Column(
            "examples_v2",
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
        after="examples",
    ),
    NewColumn(
        column=Column(
            "profiling_type",
            String(Modifiers(low_cardinality=True, default="'transaction'")),
        ),
        after="is_application",
    ),
]

all_columns = columns + [col.column for col in new_columns]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    data_granularity = 60 * 60  # 1 hour buckets
    storage_set = StorageSetKey.FUNCTIONS

    local_raw_table = "functions_raw_local"
    dist_raw_table = "functions_raw_dist"

    local_materialized_table = "functions_mv_local"
    dist_materialized_table = "functions_mv_dist"

    local_view_table = "functions_v2_local"  # newer materialized

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=new_column.column,
                after=new_column.after,
                target=target,
            )
            for table_name, target in [
                (self.local_materialized_table, OperationTarget.LOCAL),
                (self.dist_materialized_table, OperationTarget.DISTRIBUTED),
            ]
            for new_column in new_columns
        ] + [
            operations.CreateMaterializedView(
                storage_set=self.storage_set,
                view_name=self.local_view_table,
                destination_table_name=self.local_materialized_table,
                columns=all_columns,
                query=self.__MATVIEW_STATEMENT,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=self.storage_set,
                table_name=self.local_view_table,
                target=operations.OperationTarget.LOCAL,
            )
        ] + [
            operations.DropColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column_name=new_column.column.name,
                target=target,
            )
            for table_name, target in [
                (self.dist_materialized_table, OperationTarget.DISTRIBUTED),
                (self.local_materialized_table, OperationTarget.LOCAL),
            ]
            for new_column in new_columns
        ]

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
                profiling_type,
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
                argMaxState(profile_id, duration) as worst,
                argMaxState((profile_id, thread_id, start_timestamp, end_timestamp), duration) as worst_v2,
                groupUniqArrayState(5)(profile_id) as examples,
                groupUniqArrayState(5)((profile_id, thread_id, start_timestamp, end_timestamp)) as examples_v2
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
                profiling_type,
                platform,
                environment,
                release,
                os_name,
                os_version,
                timestamp,
                retention_days
        """
