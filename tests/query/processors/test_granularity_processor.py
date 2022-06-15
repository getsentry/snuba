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
from snuba.query.exceptions import InvalidGranularityException
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.processors.granularity_processor import GranularityProcessor
from snuba.query.query_settings import HTTPQuerySettings


@pytest.mark.parametrize(
    "entity_key,column",
    [
        (EntityKey.METRICS_COUNTERS, "value"),
        (EntityKey.METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.METRICS_SETS, "value"),
    ],
)
@pytest.mark.parametrize(
    "requested_granularity, query_granularity",
    [
        (None, DEFAULT_GRANULARITY),
        (10, 10),
        (60, 60),
        (90, 10),
        (120, 60),
        (60 * 60, 3600),
        (90 * 60, 60),
        (120 * 60, 3600),
        (24 * 60 * 60, 86400),
        (32 * 60 * 60, 3600),
        (48 * 60 * 60, 86400),
        (13, None),
        (0, None),
    ],
)
def test_granularity_added(
    entity_key: EntityKey,
    column: str,
    requested_granularity: Optional[int],
    query_granularity: int,
) -> None:
    query = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            ConditionFunctions.EQ, Column(None, None, "metric_id"), Literal(None, 123)
        ),
        granularity=(requested_granularity),
    )

    try:
        GranularityProcessor().process_query(query, HTTPQuerySettings())
    except InvalidGranularityException:
        assert query_granularity is None
    else:
        assert query == Query(
            QueryEntity(entity_key, ColumnSet([])),
            selected_columns=[SelectedExpression(column, Column(None, None, column))],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "granularity"),
                    Literal(None, query_granularity),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "metric_id"),
                    Literal(None, 123),
                ),
            ),
            granularity=(requested_granularity),
        )
