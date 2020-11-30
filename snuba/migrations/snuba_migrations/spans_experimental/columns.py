from typing import Sequence

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE
from snuba.clickhouse.columns import UUID, Column, DateTime, Nested, String, UInt
from snuba.migrations.columns import MigrationModifiers as Modifiers

tags_col = Column[Modifiers]("tags", Nested([("key", String()), ("value", String())]))

UNKNOWN_SPAN_STATUS = SPAN_STATUS_NAME_TO_CODE["unknown"]

columns: Sequence[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_id", UUID()),
    Column("trace_id", UUID()),
    Column("transaction_span_id", UInt(64)),
    Column("span_id", UInt(64, Modifiers(codecs=["NONE"]))),
    Column("parent_span_id", UInt(64, Modifiers(nullable=True))),
    Column("transaction_name", String(Modifiers(low_cardinality=True))),
    Column("description", String()),  # description in span
    Column("op", String(Modifiers(low_cardinality=True))),
    Column("status", UInt(8, Modifiers(default=str(UNKNOWN_SPAN_STATUS)))),
    Column("start_ts", DateTime()),
    Column("start_ns", UInt(32, Modifiers(codecs=["LZ4"]))),
    Column("finish_ts", DateTime()),
    Column("finish_ns", UInt(32, Modifiers(codecs=["LZ4"]))),
    Column("duration_ms", UInt(32)),
    tags_col,
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]
