from datetime import timedelta
from typing import Mapping, Sequence

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets.schemas.tables import MergeTreeSchema, MigrationSchemaColumn
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension


def outcomes_raw_migrations(
    clickhouse_table: str, current_schema: Mapping[str, MigrationSchemaColumn]
) -> Sequence[str]:
    # Add/remove known migrations
    ret = []
    if "size" not in current_schema:
        ret.append(f"ALTER TABLE {clickhouse_table} ADD COLUMN size Nullable(UInt32)")

    return ret


class OutcomesRawDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        read_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("key_id", Nullable(UInt(64))),
                ("timestamp", DateTime()),
                ("outcome", UInt(8)),
                ("reason", LowCardinality(Nullable(String()))),
                ("event_id", Nullable(UUID())),
                ("size", Nullable(UInt(32))),
            ]
        )

        read_schema = MergeTreeSchema(
            columns=read_columns,
            local_table_name="outcomes_raw_local",
            dist_table_name="outcomes_raw_dist",
            order_by="(org_id, project_id, timestamp)",
            partition_by="(toMonday(timestamp))",
            settings={"index_granularity": 16384},
            migration_function=outcomes_raw_migrations,
        )

        dataset_schemas = DatasetSchemas(
            read_schema=read_schema, write_schema=None, intermediary_schemas=[]
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            time_group_columns={"time": "timestamp"},
            time_parse_columns=("timestamp",),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="timestamp",
            ),
            "organization": OrganizationExtension(),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "org_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            PrewhereProcessor(),
        ]
