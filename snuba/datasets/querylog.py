from datetime import timedelta
from snuba import settings
from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    Enum,
    Float,
    LowCardinality,
    Mapping,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.querylog_processor import QuerylogProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor
from snuba.query.timeseries import TimeSeriesExtension


class QuerylogDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        status_type = Enum([("success", 0), ("error", 1), ("rate-limited", 2)])

        columns = ColumnSet(
            [
                ("request", String()),
                ("referrer", LowCardinality(String())),
                ("dataset", LowCardinality(String())),
                ("projects", Array(UInt(64))),
                ("timestamp", DateTime()),
                ("duration_ms", UInt(32)),
                ("status", status_type),
                (
                    "clickhouse_queries",
                    Nested(
                        [
                            ("sql", String()),
                            ("status", status_type),
                            ("final", UInt(8)),
                            ("cache_hit", UInt(8)),
                            ("sample", Float(32)),
                            ("max_threads", UInt(8)),
                            ("duration_ms", UInt(32)),
                            ("trace_id", Nullable(UUID())),
                        ]
                    ),
                ),
            ]
        )

        schema = MergeTreeSchema(
            columns=columns,
            local_table_name="querylog_local",
            dist_table_name="querylog_dist",
            order_by="(timestamp)",
            partition_by="(toMonday(timestamp))",
        )

        dataset_schemas = DatasetSchemas(
            read_schema=schema, write_schema=schema, intermediary_schemas=[]
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=TableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=QuerylogProcessor(), default_topic=settings.QUERIES_TOPIC,
                ),
            ),
            time_group_columns={"time": "timestamp"},
            time_parse_columns=("timestamp",),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectExtensionProcessor(project_column="project_id")
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="start_ts",
            ),
        }
