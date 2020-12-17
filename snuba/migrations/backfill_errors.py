"""\
Backfills the errors table from events.

This script will eventually be moved to a migration - after we have a multistorage
consumer running in all environments populating new events into both tables.

Errors replacements should be turned off while this script is running.
"""
import os
from datetime import datetime, timedelta
from typing import Any, Mapping, NamedTuple, Optional, Sequence, Union, cast
from uuid import UUID

import simplejson as json

from snuba.clickhouse.escaping import escape_identifier
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.processor import _ensure_valid_ip
from snuba.util import escape_literal

MAX_BATCH_SIZE = 50000
MAX_BATCH_WINDOW_MINUTES = int(os.environ.get("ERRORS_BACKFILL_WINDOW_MINUTES", 60))
MAX_BATCH_WINDOW = timedelta(minutes=MAX_BATCH_WINDOW_MINUTES)
BEGINNING_OF_TIME = (datetime.utcnow() - timedelta(days=90)).replace(
    hour=0, minute=0, second=0, microsecond=0
)


class Event(NamedTuple):
    event_id: str
    project_id: int
    timestamp: datetime
    platform: Optional[str]
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    ip_address: Optional[str]
    user: Optional[str]
    user_id: Optional[str]
    user_name: Optional[str]
    user_email: Optional[str]
    sdk_name: Optional[str]
    sdk_version: Optional[str]
    http_method: Optional[str]
    http_referer: Optional[str]
    tags_key: Sequence[str]
    tags_value: Sequence[str]
    contexts_key: Sequence[str]
    contexts_value: Sequence[str]
    transaction_name: Optional[str]
    partition: Optional[int]
    offset: Optional[int]
    message_timestamp: datetime
    retention_days: int
    deleted: int
    group_id: int
    primary_hash: str
    received: Optional[datetime]
    message: Optional[str]
    title: Optional[str]
    culprit: Optional[str]
    level: Optional[str]
    location: Optional[str]
    version: Optional[str]
    type: Optional[str]
    exception_stacks_type: Sequence[Optional[str]]
    exception_stacks_value: Sequence[Optional[str]]
    exception_stacks_mechanism_type: Sequence[Optional[str]]
    exception_stacks_mechanism_handled: Sequence[Optional[int]]
    exception_frames_abs_path: Sequence[Optional[str]]
    exception_frames_colno: Sequence[Optional[int]]
    exception_frames_filename: Sequence[Optional[str]]
    exception_frames_function: Sequence[Optional[str]]
    exception_frames_lineno: Sequence[Optional[int]]
    exception_frames_in_app: Sequence[Optional[int]]
    exception_frames_package: Sequence[Optional[str]]
    exception_frames_module: Sequence[Optional[str]]
    exception_frames_stack_level: Sequence[Optional[int]]
    sdk_integrations: Sequence[str]
    modules_name: Sequence[str]
    modules_version: Sequence[str]

    @classmethod
    def field_names(self) -> Sequence[str]:
        return [
            "event_id",
            "project_id",
            "timestamp",
            "platform",
            "environment",
            "sentry:release",
            "sentry:dist",
            "ip_address",
            "sentry:user",
            "user_id",
            "username",
            "email",
            "sdk_name",
            "sdk_version",
            "http_method",
            "http_referer",
            "tags.key",
            "tags.value",
            "contexts.key",
            "contexts.value",
            "transaction",
            "partition",
            "offset",
            "message_timestamp",
            "retention_days",
            "deleted",
            "group_id",
            "primary_hash",
            "received",
            "message",
            "title",
            "culprit",
            "level",
            "location",
            "version",
            "type",
            "exception_stacks.type",
            "exception_stacks.value",
            "exception_stacks.mechanism_type",
            "exception_stacks.mechanism_handled",
            "exception_frames.abs_path",
            "exception_frames.colno",
            "exception_frames.filename",
            "exception_frames.function",
            "exception_frames.lineno",
            "exception_frames.in_app",
            "exception_frames.package",
            "exception_frames.module",
            "exception_frames.stack_level",
            "sdk_integrations",
            "modules.name",
            "modules.version",
        ]


class Error(NamedTuple):
    event_id: UUID
    project_id: int
    timestamp: datetime
    platform: str
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    ip_address_v4: Optional[str]
    ip_address_v6: Optional[str]
    user: str
    user_id: Optional[str]
    user_name: Optional[str]
    user_email: Optional[str]
    sdk_name: Optional[str]
    sdk_version: Optional[str]
    http_method: Optional[str]
    http_referer: Optional[str]
    tags_key: Sequence[str]
    tags_value: Sequence[str]
    contexts_key: Sequence[str]
    contexts_value: Sequence[str]
    transaction_name: str
    span_id: Optional[int]
    trace_id: Optional[UUID]
    partition: int
    offset: int
    message_timestamp: datetime
    retention_days: int
    deleted: int
    group_id: int
    primary_hash: UUID
    received: Union[datetime, int]
    message: str
    title: str
    culprit: str
    level: Optional[str]
    location: Optional[str]
    version: Optional[str]
    type: str
    exception_stacks_type: Sequence[Optional[str]]
    exception_stacks_value: Sequence[Optional[str]]
    exception_stacks_mechanism_type: Sequence[Optional[str]]
    exception_stacks_mechanism_handled: Sequence[Optional[int]]
    exception_frames_abs_path: Sequence[Optional[str]]
    exception_frames_colno: Sequence[Optional[int]]
    exception_frames_filename: Sequence[Optional[str]]
    exception_frames_function: Sequence[Optional[str]]
    exception_frames_lineno: Sequence[Optional[int]]
    exception_frames_in_app: Sequence[Optional[int]]
    exception_frames_package: Sequence[Optional[str]]
    exception_frames_module: Sequence[Optional[str]]
    exception_frames_stack_level: Sequence[Optional[int]]
    sdk_integrations: Sequence[str]
    modules_name: Sequence[str]
    modules_version: Sequence[str]

    @classmethod
    def field_names(self) -> Sequence[str]:
        return [
            "event_id",
            "project_id",
            "timestamp",
            "platform",
            "environment",
            "release",
            "dist",
            "ip_address_v4",
            "ip_address_v6",
            "user",
            "user_id",
            "user_name",
            "user_email",
            "sdk_name",
            "sdk_version",
            "http_method",
            "http_referer",
            "tags.key",
            "tags.value",
            "contexts.key",
            "contexts.value",
            "transaction_name",
            "span_id",
            "trace_id",
            "partition",
            "offset",
            "message_timestamp",
            "retention_days",
            "deleted",
            "group_id",
            "primary_hash",
            "received",
            "message",
            "title",
            "culprit",
            "level",
            "location",
            "version",
            "type",
            "exception_stacks.type",
            "exception_stacks.value",
            "exception_stacks.mechanism_type",
            "exception_stacks.mechanism_handled",
            "exception_frames.abs_path",
            "exception_frames.colno",
            "exception_frames.filename",
            "exception_frames.function",
            "exception_frames.lineno",
            "exception_frames.in_app",
            "exception_frames.package",
            "exception_frames.module",
            "exception_frames.stack_level",
            "sdk_integrations",
            "modules.name",
            "modules.version",
        ]


def process_event(event_data: Sequence[Any]) -> Mapping[str, Any]:
    event = Event(*event_data)
    ip_v4 = None
    ip_v6 = None
    ip_address = _ensure_valid_ip(event.ip_address)

    if ip_address:
        if ip_address.version == 4:
            ip_v4 = str(ip_address)
        elif ip_address.version == 6:
            ip_v6 = str(ip_address)

    # Extract promoted contexts
    trace_id = None
    span_id = None

    try:
        trace_idx = event.contexts_key.index("trace")
        transaction_ctx = json.loads(event.contexts_value[trace_idx])
    except ValueError:
        transaction_ctx = {}

    if transaction_ctx.get("trace_id", None):
        trace_id = UUID(transaction_ctx["trace_id"])
    if transaction_ctx.get("span_id", None):
        span_id = int(transaction_ctx["span_id"], 16)

    error = Error(
        UUID(event.event_id),
        event.project_id,
        event.timestamp,
        event.platform or "",
        event.environment,
        event.release,
        event.dist,
        ip_v4,
        ip_v6,
        event.user or "",
        event.user_id,
        event.user_name,
        event.user_email,
        event.sdk_name,
        event.sdk_version,
        event.http_method,
        event.http_referer,
        event.tags_key,
        event.tags_value,
        event.contexts_key,
        event.contexts_value,
        event.transaction_name or "",
        span_id,
        trace_id,
        event.partition or 0,
        event.offset or 0,
        event.message_timestamp,
        event.retention_days,
        event.deleted,
        event.group_id,
        UUID(event.primary_hash),
        event.received or 0,
        event.message or "",
        event.title or "",
        event.culprit or "",
        event.level,
        event.location,
        event.version,
        event.type or "",
        event.exception_stacks_type,
        event.exception_stacks_value,
        event.exception_stacks_mechanism_type,
        event.exception_stacks_mechanism_handled,
        event.exception_frames_abs_path,
        event.exception_frames_colno,
        event.exception_frames_filename,
        event.exception_frames_function,
        event.exception_frames_lineno,
        event.exception_frames_in_app,
        event.exception_frames_package,
        event.exception_frames_module,
        event.exception_frames_stack_level,
        event.sdk_integrations,
        event.modules_name,
        event.modules_version,
    )

    return {
        name: value
        for name, value in zip(Error.field_names(), error._asdict().values())
    }


class MigratedEvent(NamedTuple):
    event_id: str
    project_id: int
    timestamp: datetime

    def __repr__(self) -> str:
        return f"{self.event_id} {self.project_id} {self.timestamp}"


def backfill_errors() -> None:
    events_storage = get_writable_storage(StorageKey.EVENTS)
    errors_storage = get_writable_storage(StorageKey.ERRORS)

    cluster = events_storage.get_cluster()

    clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)

    events_table_name = events_storage.get_table_writer().get_schema().get_table_name()
    errors_table_name = errors_storage.get_table_writer().get_schema().get_table_name()

    # Either the last event that was migrated, or the last timestamp we've checked
    # after which all events have already been migrated
    last_migrated: Union[MigratedEvent, datetime]

    try:
        (event_id, project, timestamp) = clickhouse.execute(
            f"""
                SELECT replaceAll(toString(event_id), '-', ''), project_id, timestamp FROM {errors_table_name}
                WHERE type != 'transaction'
                AND NOT deleted
                ORDER BY timestamp ASC, project_id ASC, replaceAll(toString(event_id), '-', '') ASC
                LIMIT 1
            """
        )[0]

        last_migrated = MigratedEvent(event_id, project, timestamp)

        print(f"Error data was found; starting migration from {last_migrated}")
    except IndexError:
        last_migrated = datetime.utcnow()

    src_columns = ", ".join(
        [cast(str, escape_identifier(field)) for field in Event.field_names()]
    )

    events_migrated = 0

    while True:
        if isinstance(last_migrated, MigratedEvent):
            event_conditions = f"""
            AND (
                (
                    timestamp = {escape_literal(last_migrated.timestamp)}
                    AND (
                        project_id < {escape_literal(last_migrated.project_id)}
                        OR (
                            project_id = {escape_literal(last_migrated.project_id)}
                            AND event_id < {escape_literal(last_migrated.event_id)}
                        )
                    )
                )
                OR (
                    timestamp < {escape_literal(last_migrated.timestamp)}
                )
            )
            AND timestamp >= {escape_literal(last_migrated.timestamp - MAX_BATCH_WINDOW)}
            """
        else:
            event_conditions = f"""
            AND timestamp >= {escape_literal(last_migrated - MAX_BATCH_WINDOW)}
            AND timestamp <= {escape_literal(last_migrated)}
            """

        raw_events = clickhouse.execute(
            f"""
            SELECT {src_columns} FROM {events_table_name}
            WHERE type != 'transaction'
            AND NOT deleted
            {event_conditions}
            ORDER BY timestamp DESC, project_id DESC, event_id DESC
            LIMIT {MAX_BATCH_SIZE}
            """
        )

        if len(raw_events) == 0:
            if isinstance(last_migrated, datetime):
                if last_migrated - MAX_BATCH_WINDOW < BEGINNING_OF_TIME:
                    print(f"Done. Migrated {events_migrated} events")
                    return
                else:
                    last_migrated -= MAX_BATCH_WINDOW
            else:
                last_migrated = last_migrated.timestamp - MAX_BATCH_WINDOW
        else:
            (event_id, project, timestamp,) = raw_events[-1][0:3]
            last_migrated = MigratedEvent(event_id, project, timestamp)

            data = [process_event(event) for event in raw_events]

            clickhouse.execute(
                f"INSERT INTO {errors_table_name} FORMAT JSONEachRow", data
            )

            print(f"Copied {len(data)} events; last migrated event: {last_migrated}")

            events_migrated += len(data)
