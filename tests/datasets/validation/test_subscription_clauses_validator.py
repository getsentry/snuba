from typing import cast

import pytest

from snuba.datasets.entities import EntityKeys
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import SubscriptionAllowedClausesValidator
from snuba.subscriptions.entity_subscription import (
    ENTITY_KEY_TO_SUBSCRIPTION_MAPPER,
    EntitySubscription,
    EntitySubscriptionValidation,
)


class EntityKeySubscription(EntitySubscriptionValidation, EntitySubscription):
    ...


tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKeys.EVENTS, get_entity(EntityKeys.EVENTS).get_data_model()
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
                EntityKeys.METRICS_COUNTERS,
                get_entity(EntityKeys.METRICS_COUNTERS).get_data_model(),
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
                    binary_condition(
                        "equals",
                        SubscriptableReference(
                            "_snuba_tags[asd]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "asd"),
                        ),
                        Literal(None, 2),
                    ),
                ),
            ),
            groupby=[
                Column("_snuba_project_id", None, "project_id"),
                SubscriptableReference(
                    "_snuba_tags[asd]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "asd"),
                ),
            ],
        ),
        id="groupby is allowed in metrics counters subscriptions",
    ),
]


@pytest.mark.parametrize("query", tests)  # type: ignore
def test_subscription_clauses_validation(query: LogicalQuery) -> None:
    entity_subscription_cls = cast(
        EntityKeySubscription,
        ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[query.get_from_clause().key],
    )

    validator = SubscriptionAllowedClausesValidator(
        max_allowed_aggregations=1,
        disallowed_aggregations=entity_subscription_cls.disallowed_aggregations,
    )
    validator.validate(query)


invalid_tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKeys.EVENTS, get_entity(EntityKeys.EVENTS).get_data_model()
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
                EntityKeys.EVENTS, get_entity(EntityKeys.EVENTS).get_data_model()
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
                "greater",
                Column("_snuba_count", None, "count"),
                Literal(None, 1),
            ),
        ),
        id="no having clauses",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKeys.EVENTS, get_entity(EntityKeys.EVENTS).get_data_model()
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
                EntityKeys.METRICS_COUNTERS,
                get_entity(EntityKeys.METRICS_COUNTERS).get_data_model(),
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
                SubscriptableReference(
                    "_snuba_tags[3]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "3"),
                ),
            ],
        ),
        id="tags[3] is in the group by clause but has no matching condition",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(
                EntityKeys.METRICS_COUNTERS,
                get_entity(EntityKeys.METRICS_COUNTERS).get_data_model(),
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
                    binary_condition(
                        "equals",
                        SubscriptableReference(
                            "_snuba_tags[asd]",
                            Column("_snuba_tags", None, "tags"),
                            Literal(None, "asd"),
                        ),
                        Literal(None, 2),
                    ),
                ),
            ),
            groupby=[
                Column("_snuba_project_id", None, "project_id"),
                SubscriptableReference(
                    "_snuba_tags[3]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "3"),
                ),
            ],
        ),
        id="groupby field in where clause but with a different key",
    ),
]


@pytest.mark.parametrize("query", invalid_tests)  # type: ignore
def test_subscription_clauses_validation_failure(query: LogicalQuery) -> None:
    entity_subscription_cls = cast(
        EntityKeySubscription,
        ENTITY_KEY_TO_SUBSCRIPTION_MAPPER[query.get_from_clause().key],
    )
    validator = SubscriptionAllowedClausesValidator(
        max_allowed_aggregations=1,
        disallowed_aggregations=entity_subscription_cls.disallowed_aggregations,
    )
    with pytest.raises(InvalidQueryException):
        validator.validate(query)
