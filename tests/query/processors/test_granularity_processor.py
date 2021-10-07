from typing import Optional

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.datasets.metrics import DEFAULT_GRANULARITY
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors.granularity_processor import GranularityProcessor
from snuba.request.request_settings import HTTPRequestSettings


@pytest.mark.parametrize(
    "entity_key,column",
    [
        (EntityKey.METRICS_COUNTERS, "value"),
        (EntityKey.METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.METRICS_SETS, "value"),
    ],
)
@pytest.mark.parametrize(
    "input_granularity, output_granularity",
    [(None, DEFAULT_GRANULARITY), (10, 10), (60, 60)],
)
def test_granularity_added(
    entity_key: EntityKey,
    column: str,
    input_granularity: Optional[int],
    output_granularity: int,
) -> None:
    query = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            ConditionFunctions.EQ, Column(None, None, "metric_id"), Literal(None, 123)
        ),
        granularity=(input_granularity),
    )

    GranularityProcessor().process_query(query, HTTPRequestSettings())

    assert query == Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, output_granularity),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
        granularity=(input_granularity),
    )
