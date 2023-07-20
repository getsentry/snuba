from copy import deepcopy
from unittest import mock

import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.clickhouse.query import Query
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.physical.null_column_caster import NullColumnCaster
from snuba.query.query_settings import HTTPQuerySettings

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
    readiness_state=ReadinessState.LIMITED,
    schema=schema1,
)

Storage2 = ReadableTableStorage(
    storage_key=StorageKey.DISCOVER,
    storage_set_key=StorageSetKey.DISCOVER,
    readiness_state=ReadinessState.LIMITED,
    schema=schema2,
)


merged_columns = ColumnSet(
    [
        ("timestamp", DateTime()),
        ("mismatched1", String(Modifiers(nullable=True))),
        ("mismatched2", String(Modifiers(nullable=True))),
    ]
)


test_data = [
    pytest.param(
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None, "uniq", (Column(None, None, "mismatched1"),)
                    ),
                )
            ],
        ),
        Query(
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
        ),
        id="cast string to null",
    ),
    pytest.param(
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None, "uniq", (Column(None, None, "mismatched2"),)
                    ),
                )
            ],
        ),
        Query(
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
                                    Column(None, None, "mismatched2"),
                                    Literal(None, "Nullable(UInt64)"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
        id="cast UInt64 to null",
    ),
    pytest.param(
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None, "uniq", (Column(None, None, "not_mismatched"),)
                    ),
                )
            ],
        ),
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None, "uniq", (Column(None, None, "not_mismatched"),)
                    ),
                )
            ],
        ),
        id="don't cast non-mismatched fields",
    ),
    pytest.param(
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None,
                        "and",
                        (Column(None, None, "mismatched"), Literal(None, "True")),
                    ),
                )
            ],
        ),
        Query(
            Table("discover", merged_columns),
            selected_columns=[
                SelectedExpression(
                    name="_snuba_count_unique_sdk_version",
                    expression=FunctionCall(
                        None,
                        "and",
                        (Column(None, None, "mismatched"), Literal(None, "True")),
                    ),
                )
            ],
        ),
        id="don't cast non-aggregate functions",
    ),
]


def _mock_get_storage(storage_key: StorageKey) -> ReadableTableStorage:
    if storage_key == StorageKey("storage1"):
        return Storage1
    elif storage_key == StorageKey("storage2"):
        return Storage2
    else:
        raise Exception("UNKNOWN STORAGE KEY " + str(storage_key))


def test_find_mismatched_columns():
    with mock.patch(
        "snuba.datasets.storages.factory.get_storage",
        side_effect=_mock_get_storage,
    ):
        caster = NullColumnCaster(["storage1", "storage2"])
        assert caster.mismatched_null_columns.keys() == {"mismatched1", "mismatched2"}


@pytest.mark.parametrize("input_q, expected_q", test_data)
def test_caster(input_q, expected_q):
    with mock.patch(
        "snuba.datasets.storages.factory.get_storage",
        side_effect=_mock_get_storage,
    ):
        for caster in (
            NullColumnCaster(["storage2", "storage1"]),
            NullColumnCaster(["storage1", "storage2"]),
        ):
            input_query = deepcopy(input_q)
            caster.process_query(input_query, HTTPQuerySettings())
            assert input_query == expected_q
