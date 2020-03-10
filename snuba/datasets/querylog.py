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
from snuba.datasets.dataset import Dataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.querylog_processor import QuerylogProcessor
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.extensions import QueryExtension


class QuerylogDataset(Dataset):
    def __init__(self) -> None:
        status_type = Enum([("success", 0), ("error", 1), ("rate-limited", 2)])

        columns = ColumnSet(
            [
                ("request_id", UUID()),
                ("request_body", String()),
                ("referrer", LowCardinality(String())),
                ("dataset", LowCardinality(String())),
                ("projects", Array(UInt(64))),
                ("organization", Nullable(UInt(64))),
                ("timestamp", DateTime()),
                ("duration_ms", UInt(32)),
                ("status", status_type),
                (
                    "clickhouse_queries",
                    Nested(
                        [
                            ("sql", String()),
                            ("status", status_type),
                            ("trace_id", Nullable(UUID())),
                            ("duration_ms", UInt(32)),
                            ("stats", String()),
                        ]
                    ),
                ),
            ]
        )

        schema = MergeTreeSchema(
            columns=columns,
            local_table_name="querylog_local",
            dist_table_name="querylog_dist",
            order_by="(toStartOfDay(timestamp), request_id)",
            partition_by="(toMonday(timestamp))",
            sample_expr="request_id",
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
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {}
