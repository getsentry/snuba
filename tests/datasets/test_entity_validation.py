import datetime
import pytest
from typing import Generator, Optional

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery

tests = [
    pytest.param(
        EntityKey.GROUPASSIGNEE, None, None, id="entity that has no required columns",
    ),
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "equals", Column("_snuba_project_id", None, "project_id"), Literal(None, 1),
        ),
        binary_condition(
            "equals", Column("_snuba_project_id", None, "project_id"), Literal(None, 1),
        ),
        id="spans has project required with =",
    ),
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        binary_condition(
            "in",
            Column("_snuba_project_id", None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="spans has project required with =",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "equals",
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "equals",
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
            ),
        ),
        id="specific time conditions are valid",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "in",
                Column("_snuba_timestamp", None, "timestamp"),
                FunctionCall(
                    None, "tuple", (Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),)
                ),
            ),
        ),
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "in",
                Column("_snuba_timestamp", None, "timestamp"),
                FunctionCall(
                    None, "tuple", (Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),)
                ),
            ),
        ),
        id="filtering time with IN is allowed",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 30)),
                ),
                binary_condition(
                    "less",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 2, 0, 30)),
                ),
            ),
        ),
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                ),
                binary_condition(
                    "less",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                ),
            ),
        ),
        id="from/to aligned with date align",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 30)),
                ),
                binary_condition(
                    "less",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 20, 0, 30)),
                ),
            ),
        ),
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 15, 0, 0)),
                ),
                binary_condition(
                    "less",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 20, 0, 0)),
                ),
            ),
        ),
        id="from is aligned to max days",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 30)),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "less",
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 2, 0, 30)),
                    ),
                    binary_condition(
                        "less",
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 3, 0, 30)),
                    ),
                ),
            ),
        ),
        binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greaterOrEquals",
                    Column("_snuba_timestamp", None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "less",
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                    ),
                    binary_condition(
                        "less",
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 3, 0, 30)),
                    ),
                ),
            ),
        ),
        id="only minimum time range is adjusted",
    ),
]


@pytest.fixture(autouse=True)
def set_configs() -> Generator[None, None, None]:
    state.set_config("max_days", 5)
    state.set_config("date_align_seconds", 3600)
    yield
    state.set_config("max_days", None)
    state.set_config("date_align_seconds", 1)


@pytest.mark.parametrize("key, condition, expected_condition", tests)
def test_entity_validation(
    key: EntityKey,
    condition: Optional[Expression],
    expected_condition: Optional[Expression],
) -> None:
    entity = get_entity(key)

    def query_fn(cond: Optional[Expression]) -> LogicalQuery:
        return LogicalQuery(
            QueryEntity(key, entity.get_data_model()),
            selected_columns=[
                SelectedExpression(
                    "time", Column("_snuba_timestamp", None, "timestamp")
                ),
            ],
            condition=cond,
        )

    query = query_fn(condition)
    assert entity.validate_required_conditions(query)
    expected_query = query_fn(expected_condition)
    matched, reason = query.equals(expected_query)
    assert matched, reason


invalid_tests = [
    pytest.param(
        EntityKey.EVENTS, None, id="entity has columns, but there are no conditions",
    ),
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "notIn",
            Column(None, None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        id="spans does not have project with EQ or IN",
    ),
    pytest.param(
        EntityKey.EVENTS,
        binary_condition(
            "and",
            binary_condition(
                "equals", Column(None, None, "project_id"), Literal(None, 1),
            ),
            binary_condition(
                "and",
                binary_condition(
                    "greater",
                    Column(None, None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                ),
                binary_condition(
                    "less",
                    Column(None, None, "timestamp"),
                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                ),
            ),
        ),
        id="events does not have time with GTE and LT",
    ),
]


@pytest.mark.parametrize("key, condition", invalid_tests)
def test_entity_validation_failure(
    key: EntityKey, condition: Optional[Expression]
) -> None:
    entity = get_entity(key)
    query = LogicalQuery(
        QueryEntity(key, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    assert not entity.validate_required_conditions(query)
