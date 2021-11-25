from typing import cast

import pytest

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import SubscriptionAllowedClausesValidator
from snuba.subscriptions.entity_subscription import (
    ENTITY_KEY_TO_SUBSCRIPTION_MAPPER,
    EntitySubscription,
    EntitySubscriptionValidation,
)

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
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKey.METRICS_COUNTERS,
                get_entity(EntityKey.METRICS_COUNTERS).get_data_model(),
            ),
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
                        "equals",
                        Column("_snuba_project_id", None, "project_id"),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        "equals",
                        Column("_snuba_org_id", None, "org_id"),
                        Literal(None, 1),
                    ),
                ),
            ),
            groupby=[
                Column("_snuba_project_id", None, "project_id"),
                Column("_snuba_tags[3]", None, "tags[3]"),
            ],
        ),
        id="groupby is allowed in metrics counters subscriptions",
    ),
]


@pytest.mark.parametrize("query", tests)  # type: ignore
def test_subscription_clauses_validation(query: LogicalQuery) -> None:
    class EntityKeySubscription(EntitySubscriptionValidation, EntitySubscription):
        ...

    entity_subscription_cls = cast(
        EntityKeySubscription,
        ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[query.get_from_clause().key],
    )

    validator = SubscriptionAllowedClausesValidator(max_allowed_aggregations=1)
    validator.validate(query, disallowed=entity_subscription_cls.disallowed)


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
]


@pytest.mark.parametrize("query", invalid_tests)  # type: ignore
def test_subscription_clauses_validation_failure(query: LogicalQuery) -> None:
    validator = SubscriptionAllowedClausesValidator(max_allowed_aggregations=1)
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
