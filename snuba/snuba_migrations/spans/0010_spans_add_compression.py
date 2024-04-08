from typing import Iterator, List, Sequence

from snuba.clickhouse.columns import UUID, Array, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import (
    SENTRY_TAGS_HASH_MAP_COLUMN,
    TAGS_HASH_MAP_COLUMN,
)
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import SqlOperation
from snuba.utils.schemas import Float

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"

"""
This migration adds ZSTD compression codecs to the spans table.
"""

columns_codecs: List[Column[Modifiers]] = [
    Column("deleted", UInt(8, Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("start_timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("is_segment", UInt(8, Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("offset", UInt(64, Modifiers(codecs=["T64", "Delta", "ZSTD(1)"]))),
    Column("partition", UInt(16, Modifiers(codecs=["T64", "Delta", "ZSTD(1)"]))),
    Column("span_status", UInt(8, Modifiers(codecs=["T64", "Delta", "ZSTD(1)"]))),
    Column("duration", UInt(32, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("end_ms", UInt(16, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("start_ms", UInt(16, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("group_raw", UInt(64, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("op", String(Modifiers(low_cardinality=True, codecs=["ZSTD(1)"]))),
    Column("description", String(Modifiers(codecs=["ZSTD(1)"]))),
    Column("segment_name", String(Modifiers(default="''", codecs=["ZSTD(1)"]))),
    Column("trace_id", UUID(Modifiers(codecs=["ZSTD(1)"]))),
    Column("transaction_id", UUID(Modifiers(nullable=True, codecs=["ZSTD(1)"]))),
    Column("parent_span_id", UInt(64, Modifiers(nullable=True, codecs=["ZSTD(1)"]))),
    Column("segment_id", UInt(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("tags.key", Array(String(Modifiers(codecs=["ZSTD(1)"])))),
    Column("tags.value", Array(String(Modifiers(codecs=["ZSTD(1)"])))),
    Column(
        "_tags_hash_map",
        Array(
            UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN, codecs=["ZSTD(1)"])
        ),
    ),
    Column("sentry_tags.key", Array(String(Modifiers(codecs=["ZSTD(1)"])))),
    Column("sentry_tags.value", Array((String(Modifiers(codecs=["ZSTD(1)"]))))),
    Column(
        "_sentry_tags_hash_map",
        Array(
            UInt(64),
            Modifiers(materialized=SENTRY_TAGS_HASH_MAP_COLUMN, codecs=["ZSTD(1)"]),
        ),
    ),
    Column("user", String(Modifiers(nullable=True, codecs=["ZSTD(1)"]))),
    Column("exclusive_time", Float(64, Modifiers(codecs=["ZSTD(1)"]))),
    Column("transaction_op", String(Modifiers(nullable=True, codecs=["ZSTD(1)"]))),
    Column("module", String(Modifiers(low_cardinality=True, codecs=["ZSTD(1)"]))),
    Column(
        "platform",
        String(Modifiers(low_cardinality=True, nullable=True, codecs=["ZSTD(1)"])),
    ),
    Column(
        "measurements.key",
        Array(String(Modifiers(low_cardinality=True, codecs=["ZSTD(1)"]))),
    ),
    Column("measurements.value", Array(Float(64, Modifiers(codecs=["ZSTD(1)"])))),
]

columns_no_codecs: List[Column[Modifiers]] = [
    Column("deleted", UInt(8)),
    Column("start_timestamp", DateTime()),
    Column("is_segment", UInt(8)),
    Column("offset", UInt(64)),
    Column("partition", UInt(16)),
    Column("span_status", UInt(8)),
    Column("duration", UInt(32)),
    Column("end_ms", UInt(16)),
    Column("start_ms", UInt(16)),
    Column("group_raw", UInt(64)),
    Column("op", String(Modifiers(low_cardinality=True))),
    Column("description", String()),
    Column("segment_name", String(Modifiers(default="''"))),
    Column("trace_id", UUID()),
    Column("transaction_id", UUID(Modifiers(nullable=True))),
    Column("parent_span_id", UInt(64, Modifiers(nullable=True))),
    Column("segment_id", UInt(64)),
    Column("tags.key", Array(String())),
    Column("tags.value", Array(String())),
    Column(
        "_tags_hash_map", Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN))
    ),
    Column("sentry_tags.key", Array(String())),
    Column("sentry_tags.value", Array((String()))),
    Column(
        "_sentry_tags_hash_map",
        Array(UInt(64), Modifiers(materialized=SENTRY_TAGS_HASH_MAP_COLUMN)),
    ),
    Column("user", String(Modifiers(nullable=True))),
    Column("exclusive_time", Float(64)),
    Column("transaction_op", String(Modifiers(nullable=True))),
    Column("module", String(Modifiers(low_cardinality=True))),
    Column("platform", String(Modifiers(low_cardinality=True, nullable=True))),
    Column("measurements.key", Array(String(Modifiers(low_cardinality=True)))),
    Column("measurements.value", Array(Float(64))),
]


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds compression to the spans table.
    """

    blocking = False

    def forward_columns_iter(self) -> Iterator[operations.SqlOperation]:
        for column in columns_codecs:
            yield operations.ModifyColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=local_table_name,
                column=column,
                target=operations.OperationTarget.LOCAL,
            )
            yield operations.ModifyColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=dist_table_name,
                column=column,
                target=operations.OperationTarget.DISTRIBUTED,
            )

    def backwards_columns_iter(self) -> Iterator[operations.SqlOperation]:
        """
        There is no easy way to remove specif compression codecs from a column.
        So this ends up being a no-op.
        """
        for column in columns_no_codecs:
            yield operations.ModifyColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=dist_table_name,
                column=column,
                target=operations.OperationTarget.DISTRIBUTED,
            )
            yield operations.ModifyColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=local_table_name,
                column=column,
                target=operations.OperationTarget.LOCAL,
            )

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return list(self.forward_columns_iter())

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return list(self.backwards_columns_iter())
