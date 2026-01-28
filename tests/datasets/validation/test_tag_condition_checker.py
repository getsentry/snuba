from __future__ import annotations

import re

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import reset_dataset_factory
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import TagConditionValidator

reset_dataset_factory()

tests = [
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 1),
            ),
        ),
        None,
        id="No tag condition means no validation failures",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[count]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "count"),
                ),
                Literal(None, 419),
            ),
        ),
        InvalidQueryException("invalid tag condition on 'tags[count]': 419 must be a string"),
        id="comparing to non-string literal fails",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "in",
                SubscriptableReference(
                    "_snuba_tags[count]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "count"),
                ),
                FunctionCall(
                    None,
                    "array",
                    (
                        Literal(None, 419),
                        Literal(None, 70),
                        Literal(None, 175),
                        Literal(None, 181),
                        Literal(None, 58),
                    ),
                ),
            ),
        ),
        InvalidQueryException(
            "invalid tag condition on 'tags[count]': array literal 419 must be a string"
        ),
        id="rhs has a non-string in the array",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "in",
                SubscriptableReference(
                    "_snuba_tags[count]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "count"),
                ),
                FunctionCall(
                    None,
                    "array",
                    (
                        FunctionCall(
                            None,
                            "max",
                            (
                                Literal(
                                    None,
                                    419,
                                ),
                                Literal(
                                    None,
                                    70,
                                ),
                            ),
                        ),
                        Literal(None, 175),
                    ),
                ),
            ),
        ),
        None,
        id="complex expressions don't match",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[error.main_thread]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "error.main_thread"),
                ),
                Literal(None, 1),
            ),
        ),
        None,
        id="boolean integer 1 in tag condition is auto-converted to string",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[error.handled]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "error.handled"),
                ),
                Literal(None, 0),
            ),
        ),
        None,
        id="boolean integer 0 in tag condition is auto-converted to string",
    ),
    pytest.param(
        LogicalQuery(
            QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
            selected_columns=[
                SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
            ],
            condition=binary_condition(
                "in",
                SubscriptableReference(
                    "_snuba_tags[error.main_thread]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "error.main_thread"),
                ),
                FunctionCall(
                    None,
                    "array",
                    (
                        Literal(None, 0),
                        Literal(None, 1),
                    ),
                ),
            ),
        ),
        None,
        id="boolean integers in array are auto-converted to strings",
    ),
]


@pytest.mark.parametrize("query, exception", tests)
def test_subscription_clauses_validation(query: LogicalQuery, exception: Exception | None) -> None:
    validator = TagConditionValidator()

    if exception is not None:
        with pytest.raises(type(exception), match=re.escape(str(exception))):
            validator.validate(query)
    else:
        validator.validate(query)
