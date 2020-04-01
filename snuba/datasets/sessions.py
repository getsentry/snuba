import uuid
from datetime import timedelta, datetime
from typing import Mapping, Sequence, Optional

from snuba.clickhouse.columns import (
    AggregateFunction,
    ColumnSet,
    DateTime,
    LowCardinality,
    String,
    UInt,
    UUID,
)
from snuba.clusters import get_cluster
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.plans.single_storage import SingleStorageQueryPlanBuilder
from snuba.datasets.schemas.tables import (
    MergeTreeSchema,
    MaterializedViewSchema,
    AggregatingMergeTreeSchema,
)
from snuba.datasets.storage import (
    ReadableTableStorage,
    WritableTableStorage,
)
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.parsing import ParsingContext
from snuba.processor import (
    MAX_UINT32,
    MessageProcessor,
    ProcessedMessage,
    ProcessorAction,
    _collapse_uint16,
    _collapse_uint32,
    _ensure_valid_date,
)
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.timeseries_column_processor import TimeSeriesColumnProcessor
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension

WRITE_LOCAL_TABLE_NAME = "sessions_raw_local"
WRITE_DIST_TABLE_NAME = "sessions_raw_dist"
READ_LOCAL_TABLE_NAME = "sessions_hourly_local"
READ_DIST_TABLE_NAME = "sessions_hourly_dist"
READ_LOCAL_MV_NAME = "sessions_hourly_mv_local"
READ_DIST_MV_NAME = "sessions_hourly_mv_dist"

STATUS_MAPPING = {
    "ok": 0,
    "exited": 1,
    "crashed": 2,
    "abnormal": 3,
}
REVERSE_STATUS_MAPPING = {v: k for (k, v) in STATUS_MAPPING.items()}
NIL_UUID = "00000000-0000-0000-0000-000000000000"


class SessionsProcessor(MessageProcessor):
    def process_message(self, message, metadata=None) -> Optional[ProcessedMessage]:
        if message["duration"] is None:
            duration = None
        else:
            duration = _collapse_uint32(int(message["duration"] * 1000))

        # since duration is not nullable, the max duration means no duration
        if duration is None:
            duration = MAX_UINT32

        processed = {
            "session_id": str(uuid.UUID(message["session_id"])),
            "distinct_id": str(uuid.UUID(message.get("distinct_id") or NIL_UUID)),
            "seq": message["seq"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "retention_days": message["retention_days"],
            "duration": duration,
            "status": STATUS_MAPPING[message["status"]],
            "errors": _collapse_uint16(message["errors"]) or 0,
            "received": _ensure_valid_date(
                datetime.utcfromtimestamp(message["received"])
            ),
            "started": _ensure_valid_date(
                datetime.utcfromtimestamp(message["started"])
            ),
            "release": message["release"],
            "environment": message.get("environment") or "",
        }
        return ProcessedMessage(action=ProcessorAction.INSERT, data=[processed])


class SessionsDataset(TimeSeriesDataset):
    def __init__(self) -> None:
        all_columns = ColumnSet(
            [
                ("session_id", UUID()),
                ("distinct_id", UUID()),
                ("seq", UInt(64)),
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("retention_days", UInt(16)),
                ("duration", UInt(32)),
                ("status", UInt(8)),
                ("errors", UInt(16)),
                ("received", DateTime()),
                ("started", DateTime()),
                ("release", LowCardinality(String())),
                ("environment", LowCardinality(String())),
            ]
        )

        raw_schema = MergeTreeSchema(
            columns=all_columns,
            local_table_name=WRITE_LOCAL_TABLE_NAME,
            dist_table_name=WRITE_DIST_TABLE_NAME,
            order_by="(org_id, project_id, release, environment, started)",
            partition_by="(toMonday(started))",
            settings={"index_granularity": 16384},
        )

        read_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("started", DateTime()),
                ("release", LowCardinality(String())),
                ("environment", LowCardinality(String())),
                (
                    "duration_quantiles",
                    AggregateFunction("quantilesIf(0.5, 0.9)", UInt(32), UInt(8)),
                ),
                ("sessions", AggregateFunction("countIf", UUID(), UInt(8))),
                ("users", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("sessions_crashed", AggregateFunction("countIf", UUID(), UInt(8)),),
                ("sessions_abnormal", AggregateFunction("countIf", UUID(), UInt(8)),),
                ("sessions_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("users_crashed", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("users_abnormal", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("users_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
            ]
        )
        read_schema = AggregatingMergeTreeSchema(
            columns=read_columns,
            local_table_name=READ_LOCAL_TABLE_NAME,
            dist_table_name=READ_DIST_TABLE_NAME,
            prewhere_candidates=["project_id", "org_id"],
            order_by="(org_id, project_id, release, environment, started)",
            partition_by="(toMonday(started))",
            settings={"index_granularity": 256},
        )
        materialized_view_schema = MaterializedViewSchema(
            local_materialized_view_name=READ_LOCAL_MV_NAME,
            dist_materialized_view_name=READ_DIST_MV_NAME,
            prewhere_candidates=["project_id", "org_id"],
            columns=read_columns,
            query=f"""
                SELECT
                    org_id,
                    project_id,
                    toStartOfHour(started) as started,
                    release,
                    environment,
                    quantilesIfState(0.5, 0.9)(
                        duration,
                        duration <> {MAX_UINT32} AND status == 1
                    ) as duration_quantiles,
                    countIfState(session_id, seq == 0) as sessions,
                    uniqIfState(distinct_id, distinct_id != '{NIL_UUID}') as users,
                    countIfState(session_id, status == 2) as sessions_crashed,
                    countIfState(session_id, status == 3) as sessions_abnormal,
                    uniqIfState(session_id, errors > 0) as sessions_errored,
                    uniqIfState(distinct_id, status == 2) as users_crashed,
                    uniqIfState(distinct_id, status == 3) as users_abnormal,
                    uniqIfState(distinct_id, errors > 0) as users_errored
                FROM
                    %(source_table_name)s
                GROUP BY
                    org_id, project_id, started, release, environment
            """,
            local_source_table_name=WRITE_LOCAL_TABLE_NAME,
            local_destination_table_name=READ_LOCAL_TABLE_NAME,
            dist_source_table_name=WRITE_DIST_TABLE_NAME,
            dist_destination_table_name=READ_DIST_TABLE_NAME,
        )

        # The raw table we write onto, and that potentially we could
        # query.
        writable_storage = WritableTableStorage(
            cluster=get_cluster("sessions_raw"),
            schemas=StorageSchemas(read_schema=raw_schema, write_schema=raw_schema),
            table_writer=TableWriter(
                write_schema=raw_schema,
                stream_loader=KafkaStreamLoader(
                    processor=SessionsProcessor(), default_topic="ingest-sessions",
                ),
            ),
            query_processors=[],
        )
        # The materialized view we query aggregate data from.
        materialized_storage = ReadableTableStorage(
            cluster=get_cluster("sessions_hourly"),
            schemas=StorageSchemas(
                read_schema=read_schema,
                write_schema=None,
                intermediary_schemas=[materialized_view_schema],
            ),
            query_processors=[PrewhereProcessor()],
        )

        self.__time_group_columns = {"bucketed_started": "started"}
        super().__init__(
            storages=[writable_storage, materialized_storage],
            # TODO: Once we are ready to expose the raw data model and select whether to use
            # materialized storage or the raw one here, replace this with a custom storage
            # selector that decides when to use the materialized data.
            query_plan_builder=SingleStorageQueryPlanBuilder(
                storage=materialized_storage,
            ),
            abstract_column_set=read_schema.get_columns(),
            writable_storage=writable_storage,
            time_group_columns=self.__time_group_columns,
            time_parse_columns=("started", "received"),
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

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            BasicFunctionsProcessor(),
            TimeSeriesColumnProcessor(self.__time_group_columns),
        ]

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        full_col = super().column_expr(column_name, query, parsing_context, table_alias)
        func = None
        if column_name == "duration_quantiles":
            func = "quantilesIfMerge(0.5, 0.9)"
        elif column_name in ("sessions", "sessions_crashed", "sessions_abnormal"):
            func = "countIfMerge"
        elif column_name in (
            "users",
            "sessions_errored",
            "users_crashed",
            "users_abnormal",
            "users_errored",
        ):
            func = "uniqIfMerge"
        if func is not None:
            return "{}({})".format(func, full_col)
        return full_col
