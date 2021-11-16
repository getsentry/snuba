from datetime import datetime
from typing import Optional
from unittest.mock import patch

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
from snuba.request.request_settings import (
    HTTPRequestSettings,
    SubscriptionRequestSettings,
)


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
        GranularityProcessor().process_query(query, HTTPRequestSettings())
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


@pytest.mark.parametrize(
    "start,end,query_granularity",
    [
        (datetime(2020, 9, 1, 12, 00, 00), datetime(2020, 9, 1, 12, 00, 00), 10),
        (datetime(2020, 9, 1, 12, 00, 00), datetime(2020, 9, 1, 15, 00, 00), 60),
        (datetime(2020, 9, 1, 12, 00, 00), datetime(2020, 9, 2, 12, 00, 00), 3600),
        (datetime(2020, 9, 1, 12, 00, 00), datetime(2020, 9, 3, 12, 00, 00), 3600),
    ],
)
@patch("snuba.settings.ENABLE_DEV_FEATURES", True)
def test_subscription_query_granularity(
    start: datetime, end: datetime, query_granularity: int,
) -> None:
    # Reload factory after enable settings.ENABLE_DEV_FEATURES
    from importlib import reload

    from snuba.datasets.storages import factory

    reload(factory)

    query = Query(
        QueryEntity(EntityKey.METRICS_COUNTERS, ColumnSet([])),
        selected_columns=[SelectedExpression("value", Column(None, None, "value"))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, None, "timestamp"),
                    Literal(None, start),
                ),
                binary_condition(
                    ConditionFunctions.LT,
                    Column(None, None, "timestamp"),
                    Literal(None, end),
                ),
            ),
        ),
    )
    try:
        GranularityProcessor().process_query(
            query, SubscriptionRequestSettings(referrer="subscriptions_test")
        )
    except InvalidGranularityException:
        assert query_granularity is None
    else:
        assert str(query) == str(
            Query(
                QueryEntity(EntityKey.METRICS_COUNTERS, ColumnSet([])),
                selected_columns=[
                    SelectedExpression("value", Column(None, None, "value"))
                ],
                condition=binary_condition(
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "granularity"),
                        Literal(None, query_granularity),
                    ),
                    binary_condition(
                        BooleanFunctions.AND,
                        binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "metric_id"),
                            Literal(None, 123),
                        ),
                        binary_condition(
                            BooleanFunctions.AND,
                            binary_condition(
                                ConditionFunctions.GTE,
                                Column(None, None, "timestamp"),
                                Literal(None, start),
                            ),
                            binary_condition(
                                ConditionFunctions.LT,
                                Column(None, None, "timestamp"),
                                Literal(None, end),
                            ),
                        ),
                    ),
                ),
                granularity=query_granularity,
            )
        )
