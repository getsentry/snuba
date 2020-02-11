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
    Float,
    UUID,
)
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension


class SessionEventsRawDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        read_columns = ColumnSet(
            [
                ("event_id", UUID()),
                ("session_id", UUID()),
                ("distinct_id", UUID()),
                ("seq", UInt(64)),
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("retention_days", UInt(16)),
                ("deleted", UInt(8)),
                ("sample_rate", Float(32)),
                ("duration", Float(64)),
                ("status", UInt(8)),
                ("timestamp", DateTime()),
                ("started", DateTime()),
                ("release", LowCardinality(Nullable(String()))),
                ("environment", LowCardinality(Nullable(String()))),
                ("os", Nullable(String())),
                ("os_version", Nullable(String())),
                ("device_family", Nullable(String())),
            ]
        )

        read_schema = MergeTreeSchema(
            columns=read_columns,
            local_table_name="session_events_raw_local",
            dist_table_name="session_events_raw_dist",
            order_by="(project_id, toDate(started), seq, cityHash64(toString(event_id)))",
            partition_by="(toMonday(timestamp))",
            sample_expr="cityHash64(toString(event_id))",
            settings={"index_granularity": 16384},
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
