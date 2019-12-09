from datetime import timedelta
from typing import Mapping

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    LowCardinality,
    Nested,
    String,
    UInt,
    UUID,
)

from snuba.datasets.dataset import Dataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schemas.tables import MergeTreeSchema
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.extensions import QueryExtension
from snuba.query.timeseries import TimeSeriesExtension


class QueryLogDataset(Dataset):
    """
    Record a sample of the queries ran on snuba with stats useful to do some
    performance analysis.

    This is meant to consume the queries topic populated by Snuba itself.
    """

    def __init__(self):
        columns = ColumnSet(
            [
                ("clickhouse_query", String()),
                ("snuba_query", String()),
                ("timestamp", DateTime()),
                ("duration", UInt(32)),
                ("referrer", String()),
                # This is to assess the statistical accuracy if we sample the writes.
                # hits = 1 / sampling_rate. Thus it means the amount of queries this row
                # represents, and, as a consequence the accuracy (the higher hits, the lower
                # the accuracy).
                ("hits", UInt(32)),
                # The span this tra
                ("transaction_id", UUID()),
                ("span_id", UInt(64)),
                # Details about the query
                ("dataset", LowCardinality(String())),
                ("from_clause", LowCardinality(String())),
                ("columns", Array(LowCardinality(String()))),
                ("limit", UInt(32)),
                ("offset", UInt(32)),
                ("projects", Array(UInt(64))),
                # Further query specific fields (This should eventually be
                # removed in favor of columns once we are comfortable on the schema)
                ("tags", Nested([("key", String()), ("value", String())])),
            ]
        )

        schema = MergeTreeSchema(
            columns=columns,
            local_table_name="query_log_local",
            dist_table_name="query_log_dist",
            mandatory_conditions=[],
            prewhere_candidates=[],
            order_by="(dataset, referrer, toStartOfDay(timestamp), transaction_id)",
            partition_by="(toMonday(timestamp))",
            sample_expr=None,
            migration_function=None,
        )

        dataset_schemas = DatasetSchemas(read_schema=schema, write_schema=schema)

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=TableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=None, default_topic="snuba-queries",
                ),
            ),
        )

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }
