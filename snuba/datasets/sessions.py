import uuid
from datetime import timedelta, datetime
from typing import Mapping, Sequence, Optional

from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    LowCardinality,
    AggregateFunction,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.datasets.dataset import TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.schemas.tables import (
    MergeTreeSchema,
    MaterializedViewSchema,
    AggregatingMergeTreeSchema,
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
    "degraded": 4,
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
            "deleted": 0,
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
                # note here that deleted is really only an indicator so we can
                # figure out which values can be discarded.  Deleting of values
                # does not update the materializations.
                ("deleted", UInt(8)),
                ("received", DateTime()),
                ("started", DateTime()),
                ("release", LowCardinality(String())),
                ("environment", LowCardinality(String())),
            ]
        )

        write_schema = MergeTreeSchema(
            columns=all_columns,
            local_table_name=WRITE_LOCAL_TABLE_NAME,
            dist_table_name=WRITE_DIST_TABLE_NAME,
            order_by="(org_id, project_id, toDate(started), session_id, cityHash64(toString(session_id)))",
            partition_by="(toMonday(received))",
            sample_expr="cityHash64(toString(session_id))",
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
                ("uniq_sessions", AggregateFunction("countIf", UUID(), UInt(8))),
                ("uniq_users", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("uniq_sessions_crashed", AggregateFunction("countIf", UUID(), UInt(8))),
                (
                    "uniq_sessions_abnormal",
                    AggregateFunction("countIf", UUID(), UInt(8)),
                ),
                ("uniq_sessions_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("uniq_users_crashed", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("uniq_users_abnormal", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("uniq_users_errored", AggregateFunction("uniqIf", UUID(), UInt(8))),
            ]
        )
        read_schema = AggregatingMergeTreeSchema(
            columns=read_columns,
            local_table_name=READ_LOCAL_TABLE_NAME,
            dist_table_name=READ_DIST_TABLE_NAME,
            prewhere_candidates=["project_id", "org_id"],
            order_by="(org_id, project_id, release, environment, started)",
            partition_by="(toMonday(started))",
            settings={"index_granularity": 16384},
        )
        materialized_view = MaterializedViewSchema(
            local_materialized_view_name=READ_LOCAL_MV_NAME,
            dist_materialized_view_name=READ_DIST_MV_NAME,
            prewhere_candidates=["project_id", "org_id"],
            columns=read_columns,
            query=f"""
                SELECT
                    org_id,
                    project_id,
                    toStartOfHour(started) as hour_started,
                    release,
                    environment,
                    quantilesIfState(0.5, 0.9)(
                        duration,
                        duration <> {MAX_UINT32} AND status == 1
                    ) as duration,
                    countIfState(session_id, seq == 0) as uniq_sessions,
                    uniqIfState(distinct_id, distinct_id != '00000000-0000-0000-0000-000000000000') as uniq_users,
                    countIfState(session_id, status == 2) as uniq_sessions_crashed,
                    countIfState(session_id, status == 3) as uniq_sessions_abnormal,
                    uniqIfState(session_id, errors > 0) as uniq_sessions_errored,
                    uniqIfState(distinct_id, status == 2) as uniq_users_crashed,
                    uniqIfState(distinct_id, status == 3) as uniq_users_abnormal,
                    uniqIfState(distinct_id, errors > 0) as uniq_users_errored
                FROM
                    %(source_table_name)s
                WHERE
                    deleted == 0
                GROUP BY
                    org_id, project_id, hour_started, release, environment
            """,
            local_source_table_name=WRITE_LOCAL_TABLE_NAME,
            local_destination_table_name=READ_LOCAL_TABLE_NAME,
            dist_source_table_name=WRITE_DIST_TABLE_NAME,
            dist_destination_table_name=READ_DIST_TABLE_NAME,
        )

        dataset_schemas = DatasetSchemas(
            read_schema=read_schema,
            write_schema=write_schema,
            intermediary_schemas=[materialized_view],
        )

        table_writer = TableWriter(
            write_schema=write_schema,
            stream_loader=KafkaStreamLoader(
                processor=SessionsProcessor(), default_topic="ingest-sessions",
            ),
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=table_writer,
            time_group_columns={"bucketed_started": "started"},
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
            PrewhereProcessor(),
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
        elif column_name in ("uniq_sessions",
            "uniq_sessions_crashed",
            "uniq_sessions_abnormal"):
            func = "countIfMerge"
        elif column_name in (
            "uniq_users",
            "uniq_sessions_errored",
            "uniq_users_crashed",
            "uniq_users_abnormal",
            "uniq_users_errored",
        ):
            func = "uniqIfMerge"
        if func is not None:
            return "{}({})".format(func, full_col)
        return full_col
