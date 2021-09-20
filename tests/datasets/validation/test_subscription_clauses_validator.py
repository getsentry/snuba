import pytest

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import SubscriptionAllowedClausesValidator

tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression(
                    "time", Column("_snuba_timestamp", None, "timestamp")
                ),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
        ),
        id="no extra clauses",
    ),
]


@pytest.mark.parametrize("query", tests)  # type: ignore
def test_subscription_clauses_validation(query: LogicalQuery) -> None:
    validator = SubscriptionAllowedClausesValidator()
    validator.validate(query)


invalid_tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            groupby=[Column("_snuba_timestamp", None, "timestamp")],
        ),
        id="no groupby clauses",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            having=binary_condition(
                "greater", Column("_snuba_count", None, "count"), Literal(None, 1),
            ),
        ),
        id="no having clauses",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC, Column("_snuba_timestamp", None, "timestamp")
                )
            ],
        ),
        id="no orderby clauses",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()
            ),
            selected_columns=[
                SelectedExpression("count", FunctionCall("_snuba_count", "count", ())),
                SelectedExpression(
                    "count", FunctionCall("_snuba_count_2", "count", ())
                ),
                SelectedExpression(
                    "count", FunctionCall("_snuba_count_3", "count", ())
                ),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
        ),
        id="a maximum of two aggregations in the select are allowed",
    ),
]


@pytest.mark.parametrize("query", invalid_tests)  # type: ignore
def test_subscription_clauses_validation_failure(query: LogicalQuery) -> None:
    validator = SubscriptionAllowedClausesValidator()
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
