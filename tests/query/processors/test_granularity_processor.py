from datetime import datetime
from typing import List, Optional

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities.entity_key import EntityKey
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
from snuba.query.processors.logical.granularity_processor import (
    DEFAULT_GRANULARITY_RAW,
    DEFAULT_MAPPED_GRANULARITY_ENUM,
    PERFORMANCE_GRANULARITIES,
    GranularityProcessor,
    MappedGranularityProcessor,
)
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
        (None, DEFAULT_GRANULARITY_RAW),
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
def test_granularity_added_in_condition(
    entity_key: EntityKey,
    column: str,
    requested_granularity: Optional[int],
    query_granularity: int,
) -> None:
    query = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, requested_granularity),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
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
        # ([10, 10], [10, 10]),
        # ([60, 60], [60, 60]),
        ([90, 60], [10, 60]),
        # ([120, 10], [60, 10]),
        # ([10, 60 * 60], [10, 3600]),
        # ([90 * 60, 120 * 60], [60, 3600]),
        # ([24 * 60 * 60, 32 * 60 * 60], [86400, 3600]),
        # ([13, 10], [None, 10]),
        # ([10, 0], [10, None]),
    ],
)
def test_multiple_granularities_added_in_condition(
    entity_key: EntityKey,
    column: str,
    requested_granularity: List[int],
    query_granularity: List[int],
) -> None:
    query_with_multiple_conditions = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
            binary_condition(
                BooleanFunctions.OR,
                binary_condition(
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "granularity"),
                        Literal(None, requested_granularity[0]),
                    ),
                    binary_condition(
                        ConditionFunctions.GT,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 8, 1)),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "granularity"),
                        Literal(None, requested_granularity[1]),
                    ),
                    binary_condition(
                        ConditionFunctions.LT,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 8, 1)),
                    ),
                ),
            ),
        ),
    )

    try:
        GranularityProcessor().process_query(
            query_with_multiple_conditions, HTTPQuerySettings()
        )
    except InvalidGranularityException:
        assert query_granularity[0] is None or query_granularity[1] is None
    # else:
    #     assert query_with_multiple_conditions == Query(
    #         QueryEntity(entity_key, ColumnSet([])),
    #         selected_columns=[SelectedExpression(column, Column(None, None, column))],
    #         condition=binary_condition(
    #             BooleanFunctions.AND,
    #             binary_condition(
    #                 ConditionFunctions.EQ,
    #                 Column(None, None, "metric_id"),
    #                 Literal(None, 123),
    #             ),
    #             binary_condition(
    #                 BooleanFunctions.OR,
    #                 binary_condition(
    #                     BooleanFunctions.AND,
    #                     binary_condition(
    #                         ConditionFunctions.EQ,
    #                         Column(None, None, "granularity"),
    #                         Literal(None, query_granularity[0]),
    #                     ),
    #                     binary_condition(
    #                         ConditionFunctions.GT,
    #                         Column(None, None, "timestamp"),
    #                         Literal(None, datetime(2020, 8, 1)),
    #                     ),
    #                 ),
    #                 binary_condition(
    #                     BooleanFunctions.AND,
    #                     binary_condition(
    #                         ConditionFunctions.EQ,
    #                         Column(None, None, "granularity"),
    #                         Literal(None, query_granularity[1]),
    #                     ),
    #                     binary_condition(
    #                         ConditionFunctions.LT,
    #                         Column(None, None, "timestamp"),
    #                         Literal(None, datetime(2020, 8, 1)),
    #                     ),
    #                 ),
    #             ),
    #         ),
    #     )


@pytest.mark.parametrize(
    "entity_key,column",
    [
        (EntityKey.METRICS_COUNTERS, "value"),
        (EntityKey.METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.METRICS_SETS, "value"),
    ],
)
def test_invalid_granularity_combinations(
    entity_key: EntityKey,
    column: str,
) -> None:
    granularity_one = 60
    granularity_two = 3600
    query_with_clause_and_condition = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity_one),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
        granularity=(granularity_two),
    )

    query_with_multiple_conditions = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "granularity"),
                    Literal(None, granularity_one),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "granularity"),
                    Literal(None, granularity_two),
                ),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
    )

    with pytest.raises(InvalidGranularityException):
        GranularityProcessor().process_query(
            query_with_clause_and_condition, HTTPQuerySettings()
        )

    with pytest.raises(InvalidGranularityException):
        GranularityProcessor().process_query(
            query_with_multiple_conditions, HTTPQuerySettings()
        )


@pytest.mark.parametrize(
    "entity_key,column",
    [
        (EntityKey.GENERIC_METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.GENERIC_METRICS_SETS, "value"),
    ],
)
@pytest.mark.parametrize(
    "requested_granularity, query_granularity",
    [
        (None, 1),
        (10, None),
        (60, 1),
        (90, None),
        (120, 1),
        (60 * 60, 2),
        (90 * 60, 1),
        (120 * 60, 2),
        (24 * 60 * 60, 3),
        (32 * 60 * 60, 2),
        (48 * 60 * 60, 3),
        (13, None),
        (0, None),
    ],
)
def test_granularity_enum_mapping(
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
        MappedGranularityProcessor(
            accepted_granularities=PERFORMANCE_GRANULARITIES,
            default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
        ).process_query(query, HTTPQuerySettings())
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
    "entity_key,column",
    [
        (EntityKey.GENERIC_METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.GENERIC_METRICS_SETS, "value"),
    ],
)
@pytest.mark.parametrize(
    "requested_granularity, query_granularity",
    [
        (10, None),
        (60, 1),
        (90, None),
        (120, 1),
        (60 * 60, 2),
        (90 * 60, 1),
        (120 * 60, 2),
        (24 * 60 * 60, 3),
        (32 * 60 * 60, 2),
        (48 * 60 * 60, 3),
        (13, None),
        (0, None),
    ],
)
def test_granularity_enum_mapping_in_condition(
    entity_key: EntityKey,
    column: str,
    requested_granularity: Optional[int],
    query_granularity: int,
) -> None:
    query = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, requested_granularity),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
    )

    try:
        MappedGranularityProcessor(
            accepted_granularities=PERFORMANCE_GRANULARITIES,
            default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
        ).process_query(query, HTTPQuerySettings())
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
        )


@pytest.mark.parametrize(
    "entity_key,column",
    [
        (EntityKey.GENERIC_METRICS_DISTRIBUTIONS, "percentiles"),
        (EntityKey.GENERIC_METRICS_SETS, "value"),
    ],
)
def test_invalid_granularity_combinations_in_enum_mapping_processor(
    entity_key: EntityKey,
    column: str,
) -> None:
    granularity_one = 60
    granularity_two = 3600
    query_with_clause_and_condition = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "granularity"),
                Literal(None, granularity_one),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
        granularity=(granularity_two),
    )

    query_with_multiple_conditions = Query(
        QueryEntity(entity_key, ColumnSet([])),
        selected_columns=[SelectedExpression(column, Column(None, None, column))],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "granularity"),
                    Literal(None, granularity_one),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "granularity"),
                    Literal(None, granularity_two),
                ),
            ),
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, 123),
            ),
        ),
    )

    with pytest.raises(InvalidGranularityException):
        MappedGranularityProcessor(
            accepted_granularities=PERFORMANCE_GRANULARITIES,
            default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
        ).process_query(query_with_clause_and_condition, HTTPQuerySettings())

    with pytest.raises(InvalidGranularityException):
        MappedGranularityProcessor(
            accepted_granularities=PERFORMANCE_GRANULARITIES,
            default_granularity=DEFAULT_MAPPED_GRANULARITY_ENUM,
        ).process_query(query_with_multiple_conditions, HTTPQuerySettings())
