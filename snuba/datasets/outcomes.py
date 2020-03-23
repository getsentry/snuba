from datetime import datetime, timedelta
from typing import Mapping, Optional, Sequence
import uuid

from snuba import settings
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
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.storage import (
    ReadableTableStorage,
    WritableTableStorage,
)
from snuba.processor import (
    _ensure_valid_date,
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
    _unicodify,
)
from snuba.datasets.schemas.tables import (
    MergeTreeSchema,
    SummingMergeTreeSchema,
    MaterializedViewSchema,
)
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension


WRITE_LOCAL_TABLE_NAME = "outcomes_raw_local"
WRITE_DIST_TABLE_NAME = "outcomes_raw_dist"
READ_LOCAL_TABLE_NAME = "outcomes_hourly_local"
READ_DIST_TABLE_NAME = "outcomes_hourly_dist"


class OutcomesProcessor(MessageProcessor):
    def process_message(self, value, metadata=None) -> Optional[ProcessedMessage]:
        assert isinstance(value, dict)
        v_uuid = value.get("event_id")
        message = {
            "org_id": value.get("org_id", 0),
            "project_id": value.get("project_id", 0),
            "key_id": value.get("key_id"),
            "timestamp": _ensure_valid_date(
                datetime.strptime(value["timestamp"], settings.PAYLOAD_DATETIME_FORMAT),
            ),
            "outcome": value["outcome"],
            "reason": _unicodify(value.get("reason")),
            "event_id": str(uuid.UUID(v_uuid)) if v_uuid is not None else None,
        }

        return ProcessedMessage(action=ProcessorAction.INSERT, data=[message],)


class OutcomesDataset(TimeSeriesDataset):
    """
    Tracks event ingestion outcomes in Sentry.
    """

    def __init__(self, clickhouse_connection_config: ClickhouseConnectionConfig) -> None:
        write_columns = ColumnSet(
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

        raw_schema = MergeTreeSchema(
            columns=write_columns,
            # TODO: change to outcomes.raw_local when we add multi DB support
            local_table_name=WRITE_LOCAL_TABLE_NAME,
            dist_table_name=WRITE_DIST_TABLE_NAME,
            order_by="(org_id, project_id, timestamp)",
            partition_by="(toMonday(timestamp))",
            settings={"index_granularity": 16384},
        )

        read_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("key_id", UInt(64)),
                ("timestamp", DateTime()),
                ("outcome", UInt(8)),
                ("reason", LowCardinality(String())),
                ("times_seen", UInt(64)),
            ]
        )

        read_schema = SummingMergeTreeSchema(
            columns=read_columns,
            local_table_name=READ_LOCAL_TABLE_NAME,
            dist_table_name=READ_DIST_TABLE_NAME,
            order_by="(org_id, project_id, key_id, outcome, reason, timestamp)",
            partition_by="(toMonday(timestamp))",
            settings={"index_granularity": 256},
        )

        materialized_view_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("key_id", UInt(64)),
                ("timestamp", DateTime()),
                ("outcome", UInt(8)),
                ("reason", String()),
                ("times_seen", UInt(64)),
            ]
        )

        # TODO: Find a better way to specify a query for a materialized view
        # The problem right now is that we have a way to define our columns in a ColumnSet abstraction but the query
        # doesn't use it.
        query = """
               SELECT
                   org_id,
                   project_id,
                   ifNull(key_id, 0) AS key_id,
                   toStartOfHour(timestamp) AS timestamp,
                   outcome,
                   ifNull(reason, 'none') AS reason,
                   count() AS times_seen
               FROM %(source_table_name)s
               GROUP BY org_id, project_id, key_id, timestamp, outcome, reason
               """

        materialized_view_schema = MaterializedViewSchema(
            local_materialized_view_name="outcomes_mv_hourly_local",
            dist_materialized_view_name="outcomes_mv_hourly_dist",
            prewhere_candidates=["project_id", "org_id"],
            columns=materialized_view_columns,
            query=query,
            local_source_table_name=WRITE_LOCAL_TABLE_NAME,
            local_destination_table_name=READ_LOCAL_TABLE_NAME,
            dist_source_table_name=WRITE_DIST_TABLE_NAME,
            dist_destination_table_name=READ_DIST_TABLE_NAME,
        )

        # The raw table we write onto, and that potentially we could
        # query.
        writable_storage = WritableTableStorage(
            schemas=StorageSchemas(read_schema=raw_schema, write_schema=raw_schema),
            table_writer=TableWriter(
                write_schema=raw_schema,
                stream_loader=KafkaStreamLoader(
                    processor=OutcomesProcessor(), default_topic="outcomes",
                ),
                clickhouse_connection_config=clickhouse_connection_config,
            ),
            query_processors=[],
        )
        # The materialized view we query aggregate data from.
        materialized_storage = ReadableTableStorage(
            schemas=StorageSchemas(
                read_schema=read_schema,
                write_schema=None,
                intermediary_schemas=[materialized_view_schema],
            ),
            query_processors=[PrewhereProcessor()],
        )
        super().__init__(
            storages=[writable_storage, materialized_storage],
            query_plan_builder=SingleStorageQueryPlanBuilder(
                # TODO: Once we are ready to expose the raw data model and select whether to use
                # materialized storage or the raw one here, replace this with a custom storage
                # selector that decides when to use the materialized data.
                storage=materialized_storage,
            ),
            abstract_column_set=read_schema.get_columns(),
            writable_storage=writable_storage,
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

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
        ]
