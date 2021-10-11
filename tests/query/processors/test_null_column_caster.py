from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clickhouse.query import Query
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.null_column_caster import NullColumnCaster
from snuba.request.request_settings import HTTPRequestSettings

columns1 = ColumnSet(
    [
        ("not_mismatched", DateTime()),
        ("mismatched1", String(Modifiers(nullable=True))),
        ("mismatched2", UInt(64, Modifiers(nullable=True))),
    ]
)


columns2 = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("mismatched1", String()),  # non-nullable by default
        ("mismatched2", UInt(64, Modifiers(nullable=False))),
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


merged_columns = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("mismatched1", String(Modifiers(nullable=True))),
        ("mismatched2", String(Modifiers(nullable=False))),
    ]
)


def test_caster():
    caster = NullColumnCaster([Storage1, Storage2])
    assert caster._mismatched_null_columns.keys() == {"mismatched1", "mismatched2"}
    q = Query(
        Table("discover", merged_columns),
        selected_columns=[
            SelectedExpression(
                name="_snuba_count_unique_sdk_version",
                expression=FunctionCall(
                    None, "uniq", (Column(None, None, "mismatched1"),)
                ),
            )
        ],
    )

    expected_q = Query(
        Table("discover", merged_columns),
        selected_columns=[
            SelectedExpression(
                name="_snuba_count_unique_sdk_version",
                expression=FunctionCall(
                    None,
                    "uniq",
                    (
                        FunctionCall(
                            None,
                            "cast",
                            (
                                Column(None, None, "mismatched1"),
                                Literal(None, "Nullable(String)"),
                            ),
                        ),
                    ),
                ),
            )
        ],
    )

    caster.process_query(q, HTTPRequestSettings())

    assert q == expected_q, expected_q
