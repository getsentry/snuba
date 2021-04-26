import datetime
import pytest
from typing import Generator, Optional, Set

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.parser.validation import validate_entities_with_query

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
]


@pytest.fixture(autouse=True)  # type: ignore
def set_configs() -> Generator[None, None, None]:
    state.set_config("max_days", 5)
    state.set_config("date_align_seconds", 3600)
    yield
    state.set_config("max_days", None)
    state.set_config("date_align_seconds", 1)


@pytest.mark.parametrize("key, condition, expected_condition", tests)  # type: ignore
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
    validate_entities_with_query(query)
    expected_query = query_fn(expected_condition)
    matched, reason = query.equals(expected_query)
    assert matched, reason


invalid_tests = [
    pytest.param(
        EntityKey.EVENTS,
        None,
        {"project_id", "timestamp"},
        id="entity has columns, but there are no conditions",
    ),
    pytest.param(
        EntityKey.SPANS,
        binary_condition(
            "notIn",
            Column(None, None, "project_id"),
            FunctionCall(None, "tuple", (Literal(None, 1),)),
        ),
        {"project_id"},
        id="spans does not have project with EQ or IN",
    ),
]


@pytest.mark.parametrize("key, condition, missing", invalid_tests)  # type: ignore
def test_entity_validation_failure(
    key: EntityKey, condition: Optional[Expression], missing: Optional[Set[str]]
) -> None:
    entity = get_entity(key)
    query = LogicalQuery(
        QueryEntity(key, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=condition,
    )

    with pytest.raises(InvalidQueryException):
        validate_entities_with_query(query)
