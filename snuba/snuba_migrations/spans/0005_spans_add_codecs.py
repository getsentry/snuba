from typing import List, Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

# from snuba.utils.schemas import UUID, DateTime, Float, Nested, String
from snuba.utils.schemas import DateTime, String

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


unmodified_columns: List[Column[Modifiers]] = [
    Column("deleted", UInt(8, Modifiers(codecs=["Default"]))),
    Column("retention_days", UInt(16, Modifiers(codecs=["Default"]))),
    Column("is_segment", UInt(8, Modifiers(codecs=["Default"]))),
    Column("span_status", UInt(8, Modifiers(codecs=["Default"]))),
    Column("op", String(Modifiers(low_cardinality=True, codecs=["Default"]))),
    Column("start_timestamp", DateTime(Modifiers(codecs=["Default"]))),
    Column("segment_name", String(Modifiers(default="''", codecs=["Default"]))),
    Column("partition", UInt(16, Modifiers(codecs=["Default"]))),
    Column("duration", UInt(32, Modifiers(codecs=["Default"]))),
    Column("end_ms", UInt(16, Modifiers(codecs=["Default"]))),
    Column("start_ms", UInt(16, Modifiers(codecs=["Default"]))),
    Column("group", UInt(64, Modifiers(codecs=["Default"]))),
    Column("group_raw", UInt(64, Modifiers(codecs=["Default"]))),
    Column("offset", UInt(64, Modifiers(codecs=["Default"]))),
    Column("description", String(Modifiers(codecs=["Default"]))),
]

# ┌─name───────────────┬─best_codec─────────┬─compressed_size─┬─uncompressed_size─┬─min(data_compressed_bytes)─┐
# │ deleted            │ DEFAULT            │ 219.96 MiB      │ 48.42 GiB         │                  230649696 │
# │ span_kind          │ DEFAULT            │ 268.04 MiB      │ 48.59 GiB         │                  281057550 │
# │ platform           │ DEFAULT            │ 284.08 MiB      │ 48.59 GiB         │                  297876039 │
# │ action             │ DEFAULT            │ 291.93 MiB      │ 48.59 GiB         │                  306115976 │
# │ retention_days     │ DEFAULT            │ 439.80 MiB      │ 96.84 GiB         │                  461163662 │
# │ domain             │ DEFAULT            │ 508.71 MiB      │ 107.20 GiB        │                  533416964 │
# │ status             │ DEFAULT            │ 1.07 GiB        │ 242.11 GiB        │                 1152710016 │
# │ project_id         │ DEFAULT            │ 1.73 GiB        │ 387.37 GiB        │                 1858298851 │
# │ measurements.key   │ DEFAULT            │ 3.12 GiB        │ 388.97 GiB        │                 3349554689 │
# │ is_segment         │ DEFAULT            │ 3.72 GiB        │ 48.42 GiB         │                 3997622540 │
# │ module             │ DEFAULT            │ 3.73 GiB        │ 48.59 GiB         │                 4004362587 │
# │ group              │ DEFAULT            │ 5.36 GiB        │ 387.37 GiB        │                 5758552542 │
# │ measurements.value │ DEFAULT            │ 5.41 GiB        │ 11.24 GiB         │                 5811949568 │
# │ span_status        │ DEFAULT            │ 9.50 GiB        │ 48.42 GiB         │                10201311805 │
# │ user               │ DEFAULT            │ 11.96 GiB       │ 112.75 GiB        │                12836666477 │
# │ transaction_op     │ DEFAULT            │ 12.28 GiB       │ 594.51 GiB        │                13183313449 │
# │ end_timestamp      │ CODEC(DoubleDelta) │ 16.58 GiB       │ 193.68 GiB        │                17801337958 │
# │ tags.key           │ DEFAULT            │ 24.23 GiB       │ 1.40 TiB          │                26018272760 │
# │ op                 │ DEFAULT            │ 24.31 GiB       │ 89.43 GiB         │                26102206691 │
# │ start_timestamp    │ DEFAULT            │ 36.08 GiB       │ 193.68 GiB        │                38737410196 │
# │ segment_name       │ DEFAULT            │ 39.76 GiB       │ 447.22 GiB        │                42696532075 │
# │ partition          │ DEFAULT            │ 46.14 GiB       │ 96.84 GiB         │                49537483924 │
# │ duration           │ DEFAULT            │ 79.15 GiB       │ 193.68 GiB        │                84983403044 │
# │ end_ms             │ DEFAULT            │ 84.02 GiB       │ 92.56 GiB         │                90218403435 │
# │ start_ms           │ DEFAULT            │ 84.65 GiB       │ 92.56 GiB         │                90890945373 │
# │ group_raw          │ DEFAULT            │ 97.02 GiB       │ 349.51 GiB        │               104176463637 │
# │ offset             │ DEFAULT            │ 101.47 GiB      │ 387.37 GiB        │               108952768935 │
# │ _tags_hash_map     │ DEFAULT            │ 123.45 GiB      │ 1.42 TiB          │               132551008521 │
# │ segment_id         │ DEFAULT            │ 136.23 GiB      │ 387.37 GiB        │               146279926948 │
# │ tags.value         │ DEFAULT            │ 168.71 GiB      │ 2.26 TiB          │               181148159054 │
# │ parent_span_id     │ DEFAULT            │ 172.83 GiB      │ 435.79 GiB        │               185570184834 │
# │ trace_id           │ DEFAULT            │ 180.65 GiB      │ 774.74 GiB        │               193975984044 │
# │ transaction_id     │ DEFAULT            │ 222.24 GiB      │ 823.16 GiB        │               238625460239 │
# │ exclusive_time     │ DEFAULT            │ 236.37 GiB      │ 387.37 GiB        │               253805420785 │
# │ span_id            │ DEFAULT            │ 389.01 GiB      │ 387.37 GiB        │               417692527295 │
# │ description        │ DEFAULT            │ 638.61 GiB      │ 6.63 TiB          │               685707467118 │
# └────────────────────┴────────────────────┴─────────────────┴───────────────────┴────────────────────────────┘
codec_columns = [
    Column("deleted", UInt(8, Modifiers(codecs=["DoubleDelta"]))),
    Column("retention_days", UInt(16, Modifiers(codecs=["DoubleDelta"]))),
    Column("is_segment", UInt(8, Modifiers(codecs=["DoubleDelta"]))),
    Column("span_status", UInt(8, Modifiers(codecs=["T64", "Delta"]))),
    Column("op", String(Modifiers(low_cardinality=True, codecs=["ZSTD(1)"]))),
    Column("start_timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "ZSTD(1)"]))),
    Column("segment_name", String(Modifiers(codecs=["ZSTD(1)"]))),
    Column("partition", UInt(16, Modifiers(codecs=["T64", "Delta", "ZSTD(1)"]))),
    Column("duration", UInt(32, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("end_ms", UInt(16, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("start_ms", UInt(16, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("group", UInt(64, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("group_raw", UInt(64, Modifiers(codecs=["Delta", "ZSTD(1)"]))),
    Column("offset", UInt(64, Modifiers(codecs=["T64", "Delta", "ZSTD(1)"]))),
    Column("description", String(Modifiers(codecs=["ZSTD(1)"]))),
]

# check we have all the columns in both lists
assert [a.name == b.name for (a, b) in zip(codec_columns, unmodified_columns)]


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds group_raw column to store the hash of the raw description.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            *[
                operations.ModifyColumn(
                    storage_set=storage_set_name,
                    table_name=local_table_name,
                    column=col,
                    target=OperationTarget.LOCAL,
                )
                for col in codec_columns
            ],
            *[
                operations.ModifyColumn(
                    storage_set=storage_set_name,
                    table_name=dist_table_name,
                    column=col,
                    target=OperationTarget.DISTRIBUTED,
                )
                for col in codec_columns
            ],
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            *[
                operations.ModifyColumn(
                    storage_set=storage_set_name,
                    table_name=dist_table_name,
                    column=col,
                    target=OperationTarget.DISTRIBUTED,
                )
                for col in unmodified_columns
            ],
            *[
                operations.ModifyColumn(
                    storage_set=storage_set_name,
                    table_name=local_table_name,
                    column=col,
                    target=OperationTarget.LOCAL,
                )
                for col in unmodified_columns
            ],
        ]
