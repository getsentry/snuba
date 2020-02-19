import uuid
from datetime import timedelta, datetime
from typing import Mapping, Sequence, Optional

from snuba.datasets.dataset import TimeSeriesDataset
from snuba.clickhouse.columns import (
    ColumnSet,
    DateTime,
    LowCardinality,
    Nullable,
    String,
    UInt,
    Enum,
    Float,
    UUID,
)
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
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
            "distinct_id": str(uuid.UUID(message["distinct_id"])),
            "seq": message["seq"],
            "org_id": message["org_id"],
            "project_id": message["project_id"],
            "retention_days": message["retention_days"],
            "deleted": 0,
            "duration": duration,
            "status": STATUS_MAPPING[message["status"]],
            "timestamp": _ensure_valid_date(
                datetime.fromtimestamp(message["timestamp"])
            ),
            "started": _ensure_valid_date(datetime.fromtimestamp(message["started"])),
            "release": message.get("release"),
            "environment": message.get("environment"),
            "os": message.get("os"),
            "os_version": message.get("os_version"),
            "device_family": message.get("device_family"),
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
                ("deleted", UInt(8)),
                ("duration", UInt(32)),
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
            prewhere_candidates=["project_id"],
            order_by="(project_id, toDate(started), session_id, cityHash64(toString(session_id)))",
            partition_by="(toMonday(timestamp))",
            version_column="seq",
            sample_expr="cityHash64(toString(session_id))",
            settings={"index_granularity": 16384},
        )

        dataset_schemas = DatasetSchemas(read_schema=schema, write_schema=schema)

        table_writer = TableWriter(
            write_schema=schema,
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
