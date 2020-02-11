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
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor


class SessionDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        all_columns = ColumnSet(
            [
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
                ("os", LowCardinality(Nullable(String()))),
                ("os_version", LowCardinality(Nullable(String()))),
                ("device_family", LowCardinality(Nullable(String()))),
            ]
        )

        schema = ReplacingMergeTreeSchema(
            columns=all_columns,
            local_table_name="sessions_local",
            dist_table_name="sessions_dist",
            mandatory_conditions=[("deleted", "=", 0)],
            prewhere_candidates=[
                "project_id",
            ],
            order_by="(project_id, toDate(started), session_id, cityHash64(toString(session_id)))",
            partition_by="(toMonday(timestamp))",
            version_column="seq",
            sample_expr="cityHash64(toString(session_id))",
            settings={"index_granularity": 16384},
        )

        dataset_schemas = DatasetSchemas(schema=schema, schema=schema)

        table_writer = TableWriter(
            write_schema=schema,
            stream_loader=KafkaStreamLoader(
                processor=SessionsProcessor(),
                default_topic="sessions",
            ),
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=table_writer,
            time_group_columns={"bucketed_started": "started"},
            time_parse_columns=("started", "timestamp"),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=7),
                timestamp_column="started",
            ),
            "organization": OrganizationExtension(),
            "project": ProjectExtension(
                processor=ProjectExtensionProcessor(project_column="project_id")
            ),
        }

    def get_prewhere_keys(self) -> Sequence[str]:
        return ["project_id", "org_id"]

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            PrewhereProcessor(),
        ]
