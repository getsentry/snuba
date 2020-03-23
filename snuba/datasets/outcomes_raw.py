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
from snuba.clickhouse.config import ClickhouseConnectionConfig
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storage import WritableTableStorage
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension


class OutcomesRawDataset(TimeSeriesDataset):
    def __init__(self, clickhouse_connection_config: ClickhouseConnectionConfig) -> None:
        read_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("key_id", Nullable(UInt(64))),
                ("timestamp", DateTime()),
                ("outcome", UInt(8)),
                ("reason", LowCardinality(Nullable(String()))),
                ("event_id", Nullable(UUID())),
            ]
        )

        read_schema = MergeTreeSchema(
            columns=read_columns,
            local_table_name="outcomes_raw_local",
            dist_table_name="outcomes_raw_dist",
            order_by="(org_id, project_id, timestamp)",
            partition_by="(toMonday(timestamp))",
            settings={"index_granularity": 16384},
        )

        storage = WritableTableStorage(
            schemas=StorageSchemas(
                read_schema=read_schema, write_schema=None, intermediary_schemas=[]
            ),
            table_writer=None,
            query_processors=[PrewhereProcessor()],
        )

        super().__init__(
            storages=[storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(storage=storage),
            abstract_column_set=read_schema.get_columns(),
            writable_storage=None,
            time_group_columns={"time": "timestamp"},
            time_parse_columns=("timestamp",),
            clickhouse_connection_config=clickhouse_connection_config,
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
        ]
