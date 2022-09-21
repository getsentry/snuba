from datetime import datetime
from typing import List, Optional, Tuple

import pytest

from snuba import settings
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.events import v2_selector_function
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.logical import Query
from snuba.state import set_config

query = Query(
    QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
    selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
    condition=binary_condition(
        ConditionFunctions.EQ,
        Column("_snuba_project_id", None, "project_id"),
        Literal(None, 1),
    ),
)

TESTS = [
    pytest.param(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, datetime(2022, 1, 1, 0, 0, 0)),
        ),
        None,
        1.0,
        1.0,
        ("errors_v1", []),
        id="No beginning of time, disabled.",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, datetime(2021, 1, 1, 0, 0, 0)),
        ),
        datetime(2022, 1, 1, 0, 0, 0),
        1.0,
        1.0,
        ("errors_v1", []),
        id="Out of time range, disabled.",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, datetime(2022, 1, 1, 0, 0, 0)),
        ),
        datetime(2021, 1, 1, 0, 0, 0),
        1.0,
        0.0,
        ("errors_v1", ["errors_v2"]),
        id="In range, run both.",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, datetime(2022, 1, 1, 0, 0, 0)),
        ),
        datetime(2021, 1, 1, 0, 0, 0),
        1.0,
        1.0,
        ("errors_v2", ["errors_v1"]),
        id="In range, run both and trust the second.",
    ),
]


@pytest.mark.parametrize(
    "time_condition, beginning_of_time, exec_both, trust_secondary, expected_value",
    TESTS,
)
def test_selector_function(
    time_condition: Expression,
    beginning_of_time: Optional[datetime],
    exec_both: float,
    trust_secondary: float,
    expected_value: Tuple[str, List[str]],
) -> None:
    query = Query(
        QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=time_condition,
    )

    set_config("rollout_upgraded_errors_trust", trust_secondary)
    set_config("rollout_upgraded_errors_execute", exec_both)

    previous_time = settings.ERRORS_UPGRADE_BEGINING_OF_TIME
    settings.ERRORS_UPGRADE_BEGINING_OF_TIME = beginning_of_time

    assert v2_selector_function(query, "test") == expected_value

    settings.ERRORS_UPGRADE_BEGINING_OF_TIME = previous_time
