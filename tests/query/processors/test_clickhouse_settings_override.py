from typing import Any, MutableMapping

import pytest

from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String
from snuba.clickhouse.query import Query
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall
from snuba.query.processors.physical.clickhouse_settings_override import (
    ClickhouseSettingsOverride,
)
from snuba.query.query_settings import HTTPQuerySettings

tests = [
    pytest.param({}),
    pytest.param(
        {
            "max_rows_to_group_by": 1000000,
            "group_by_overflow_mode": "any",
        }
    ),
]


@pytest.mark.parametrize("clickhouse_settings", tests)
@pytest.mark.redis_db
def test_apply_clickhouse_settings(
    clickhouse_settings: MutableMapping[str, Any]
) -> None:
    query = Query(
        Table(
            "discover",
            ColumnSet(
                [
                    ("timestamp", DateTime()),
                    ("mismatched1", String(Modifiers(nullable=True))),
                    ("mismatched2", String(Modifiers(nullable=True))),
                ]
            ),
        ),
        selected_columns=[
            SelectedExpression(
                name="_snuba_count_unique_sdk_version",
                expression=FunctionCall(
                    None, "uniq", (Column(None, None, "mismatched1"),)
                ),
            )
        ],
    )
    settings = HTTPQuerySettings()

    ClickhouseSettingsOverride(clickhouse_settings).process_query(query, settings)
    assert settings.get_clickhouse_settings() == clickhouse_settings
