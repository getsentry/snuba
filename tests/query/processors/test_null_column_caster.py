from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.query.processors.null_column_caster import NullColumnCaster

columns1 = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("environment", String(Modifiers(nullable=True))),
        ("release", String(Modifiers(nullable=True))),
    ]
)


columns2 = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("environment", String()),
        ("release", String(Modifiers(nullable=False))),
    ]
)

schema1 = TableSchema(
    columns=columns1,
    local_table_name="discover_local",
    dist_table_name="discover_dist",
    storage_set_key=StorageSetKey.DISCOVER,
    mandatory_conditions=[],
)

schema2 = TableSchema(
    columns=columns2,
    local_table_name="discover_local",
    dist_table_name="discover_dist",
    storage_set_key=StorageSetKey.DISCOVER,
    mandatory_conditions=[],
)

Storage1 = ReadableTableStorage(
    storage_key=StorageKey.DISCOVER,
    storage_set_key=StorageSetKey.DISCOVER,
    schema=schema1,
)

Storage2 = ReadableTableStorage(
    storage_key=StorageKey.DISCOVER,
    storage_set_key=StorageSetKey.DISCOVER,
    schema=schema2,
)


def test_caster():
    import pdb

    pdb.set_trace()
    caster = NullColumnCaster([Storage1, Storage2])
    assert caster._mismatched_null_columns == {"environment", "release"}
