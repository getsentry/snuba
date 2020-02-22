import uuid
from datetime import timedelta, datetime
from typing import Mapping, Sequence, Optional

from snuba.datasets.dataset import TimeSeriesDataset
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
from snuba.datasets.schemas.tables import (
    ReplacingMergeTreeSchema,
    MaterializedViewSchema,
    AggregatingMergeTreeSchema,
)
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.processor import (
    MessageProcessor,
    ProcessorAction,
    ProcessedMessage,
    _ensure_valid_date,
    _collapse_uint32,
    MAX_UINT32,
)
from snuba.query.extensions import QueryExtension
from snuba.query.organization_extension import OrganizationExtension
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectExtensionProcessor


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
        action_type = ProcessorAction.INSERT
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
            "timestamp": _ensure_valid_date(
                datetime.utcfromtimestamp(message["timestamp"])
            ),
            "started": _ensure_valid_date(
                datetime.utcfromtimestamp(message["started"])
            ),
            "release": message.get("release"),
            "environment": message.get("environment"),
        }
        return ProcessedMessage(action=action_type, data=[processed])


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
                ("duration", UInt(32)),
                ("status", UInt(8)),
                ("timestamp", DateTime()),
                ("started", DateTime()),
                ("release", LowCardinality(Nullable(String()))),
                ("environment", LowCardinality(Nullable(String()))),
            ]
        )

        write_schema = ReplacingMergeTreeSchema(
            columns=all_columns,
            local_table_name="sessions_raw_local",
            dist_table_name="sessions_raw_dist",
            prewhere_candidates=["project_id", "org_id"],
            order_by="(org_id, project_id, toDate(started), session_id, cityHash64(toString(session_id)))",
            partition_by="(toMonday(timestamp))",
            version_column="seq",
            sample_expr="cityHash64(toString(session_id))",
            settings={"index_granularity": 16384},
        )

        read_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("started", DateTime()),
                ("release", LowCardinality(Nullable(String()))),
                ("environment", LowCardinality(Nullable(String()))),
                ("uniq_sessions", AggregateFunction("uniq", UUID())),
                ("uniq_users", AggregateFunction("uniq", UUID())),
                ("uniq_crashes", AggregateFunction("uniqIf", UUID(), UInt(8))),
                ("uniq_degraded", AggregateFunction("uniqIf", UUID(), UInt(8))),
            ]
        )
        read_schema = AggregatingMergeTreeSchema(
            columns=read_columns,
            local_table_name="sessions_aggr_local",
            dist_table_name="sessions_aggr_dist",
            prewhere_candidates=["project_id", "org_id"],
            order_by="(org_id, project_id, started, release)",
            partition_by="started",
            settings={"index_granularity": 16384},
        )
        materialized_view = MaterializedViewSchema(
            local_materialized_view_name="sessions_aggr_daily_mv_local",
            dist_materialized_view_name="sessions_aggr_daily_mv_dist",
            prewhere_candidates=["project_id", "org_id"],
            columns=read_columns,
            query="""
                SELECT
                    org_id,
                    project_id,
                    toStartOfDay(started) as started,
                    release,
                    environment,
                    uniqState(session_id) as uniq_sessions,
                    uniqState(distinct_id) as uniq_users,
                    uniqIfState(distinct_id, status == 2 OR status == 3) as uniq_crashes,
                    uniqIfState(distinct_id, status == 4) as uniq_degraded
                FROM
                    %(source_table_name)s
                GROUP BY
                    org_id, project_id, started, release, environment
            """,
            local_source_table_name="sessions_raw_local",
            local_destination_table_name="sessions_aggr_local",
            dist_source_table_name="sessions_raw_dist",
            dist_destination_table_name="sessions_aggr_dist",
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
