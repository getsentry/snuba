from typing import Any, Mapping, Optional, Type, Union

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity_subscriptions.processors import AddColumnCondition
from snuba.datasets.entity_subscriptions.validators import AggregationValidator
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query

TESTS = [
    pytest.param(
        EntityKey.EVENTS,
        Query(
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
        {},
        None,
        5,
        id="Events subscription",
    ),
    pytest.param(
        EntityKey.TRANSACTIONS,
        Query(
            QueryEntity(
                EntityKey.TRANSACTIONS,
                get_entity(EntityKey.TRANSACTIONS).get_data_model(),
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
        {},
        None,
        5,
        id="Transactions subscription",
    ),
    pytest.param(
        EntityKey.SESSIONS,
        Query(
            QueryEntity(
                EntityKey.SESSIONS,
                get_entity(EntityKey.SESSIONS).get_data_model(),
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
        {"organization": 1},
        None,
        5,
        id="Sessions subscription",
    ),
    pytest.param(
        EntityKey.SESSIONS,
        Query(
            QueryEntity(
                EntityKey.SESSIONS,
                get_entity(EntityKey.SESSIONS).get_data_model(),
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
        {},
        InvalidQueryException,
        5,
        id="Sessions subscription",
    ),
    pytest.param(
        EntityKey.METRICS_COUNTERS,
        Query(
            QueryEntity(
                EntityKey.METRICS_COUNTERS,
                get_entity(EntityKey.METRICS_COUNTERS).get_data_model(),
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
        {"organization": 1},
        None,
        5,
        id="Metrics counters subscription",
    ),
    pytest.param(
        EntityKey.METRICS_COUNTERS,
        Query(
            QueryEntity(
                EntityKey.METRICS_COUNTERS,
                get_entity(EntityKey.METRICS_COUNTERS).get_data_model(),
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
        {},
        InvalidQueryException,
        5,
        id="Metrics counters subscription",
    ),
    pytest.param(
        EntityKey.METRICS_SETS,
        Query(
            QueryEntity(
                EntityKey.METRICS_SETS,
                get_entity(EntityKey.METRICS_SETS).get_data_model(),
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
        {"organization": 1},
        None,
        5,
        id="Metrics sets subscription",
    ),
    pytest.param(
        EntityKey.METRICS_SETS,
        Query(
            QueryEntity(
                EntityKey.METRICS_SETS,
                get_entity(EntityKey.METRICS_SETS).get_data_model(),
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
        {},
        InvalidQueryException,
        5,
        id="Metrics sets subscription",
    ),
]


@pytest.mark.parametrize("entity_key, query, metadata, exception, offset", TESTS)
def test_entity_subscription_processors(
    entity_key: EntityKey,
    query: Union[CompositeQuery[QueryEntity], Query],
    metadata: Mapping[str, Any],
    exception: Optional[Type[Exception]],
    offset: Optional[int],
) -> None:
    entity_subscription = get_entity(entity_key).get_entity_subscription()

    if entity_subscription and entity_subscription.processors:
        for processor in entity_subscription.processors:
            if exception is not None:
                with pytest.raises(exception):
                    processor.to_dict(metadata) == {}
            else:
                if isinstance(processor, AddColumnCondition):
                    processor.process(query, metadata, offset)
                    new_condition = query.get_condition()
                    assert isinstance(new_condition, FunctionCall)
                    assert len(new_condition.parameters) == 2
                    assert processor.to_dict(metadata) == {
                        processor.extra_condition_data_key: 1
                    }


@pytest.mark.parametrize("entity_key, query, metadata, exception, offset", TESTS)
def test_entity_subscription_validators(
    entity_key: EntityKey,
    query: Union[CompositeQuery[QueryEntity], Query],
    metadata: Mapping[str, Any],
    exception: Optional[Type[Exception]],
    offset: Optional[int],
) -> None:
    entity_subscription = get_entity(entity_key).get_entity_subscription()

    if entity_subscription and entity_subscription.validators:
        for validator in entity_subscription.validators:
            if isinstance(validator, AggregationValidator):
                validator.validate(query)
