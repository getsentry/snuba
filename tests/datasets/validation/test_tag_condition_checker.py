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
        None,  # Now auto-coerces instead of failing
        id="comparing to non-string literal is auto-coerced",
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
        None,  # Now auto-coerces instead of failing
        id="rhs has non-strings in array that are auto-coerced",
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
                    "_snuba_tags[issue.id]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "issue.id"),
                ),
                Literal(None, 6868442908),  # Integer that should be coerced
            ),
        ),
        None,  # Should NOT raise exception after fix
        id="integer tag value is auto-coerced to string",
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
                Literal(None, 3.14159),  # Float that should be coerced
            ),
        ),
        None,  # Should NOT raise exception after fix
        id="float tag value is auto-coerced to string",
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
                    "_snuba_tags[flag]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "flag"),
                ),
                Literal(None, True),  # Boolean that should be coerced
            ),
        ),
        None,  # Should NOT raise exception after fix
        id="boolean tag value is auto-coerced to string",
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
                    "_snuba_tags[null_tag]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "null_tag"),
                ),
                Literal(None, None),  # None should STILL raise error
            ),
        ),
        InvalidQueryException("invalid tag condition on 'tags[null_tag]': None must be a string"),
        id="None value still raises error",
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
                    "_snuba_tags[issue.id]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "issue.id"),
                ),
                FunctionCall(
                    None,
                    "array",
                    (
                        Literal(None, 6868442908),  # Integer
                        Literal(None, "70"),  # String
                        Literal(None, 3.14),  # Float
                        Literal(None, True),  # Boolean
                    ),
                ),
            ),
        ),
        None,  # Should NOT raise exception - mixed types all coerced
        id="array with mixed numeric types are all coerced",
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
                    "_snuba_tags[issue.id]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "issue.id"),
                ),
                FunctionCall(
                    None,
                    "array",
                    (
                        Literal(None, 123),
                        Literal(None, None),  # None in array should still fail
                    ),
                ),
            ),
        ),
        InvalidQueryException("invalid tag condition on 'tags[issue.id]': array literal None must be a string"),
        id="None in array still raises error",
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
                Literal(None, "existing_string"),  # Existing string behavior
            ),
        ),
        None,
        id="existing string tag values still work",
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


def test_tag_condition_coercion_modifies_query_in_place() -> None:
    """Test that the validator modifies literal values in-place"""
    literal_int = Literal(None, 6868442908)
    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=binary_condition(
            "equals",
            SubscriptableReference(
                "_snuba_tags[issue.id]",
                Column("_snuba_tags", None, "tags"),
                Literal(None, "issue.id"),
            ),
            literal_int,
        ),
    )

    # Before validation: integer
    assert isinstance(literal_int.value, int)
    assert literal_int.value == 6868442908

    validator = TagConditionValidator()
    validator.validate(query)

    # After validation: string (modified in place)
    assert isinstance(literal_int.value, str)
    assert literal_int.value == "6868442908"


def test_tag_condition_coercion_with_multiple_types() -> None:
    """Test coercion works with multiple different types in same query"""
    from snuba.query.conditions import BooleanFunctions

    query = LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("time", Column("_snuba_timestamp", None, "timestamp")),
        ],
        condition=binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[issue.id]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "issue.id"),
                ),
                Literal(None, 123),
            ),
            binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[ratio]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "ratio"),
                ),
                Literal(None, 0.5),
            ),
        ),
    )

    validator = TagConditionValidator()
    validator.validate(query)

    # Both conditions should be coerced - verify by checking they don't raise errors
    # The query should now be valid
    condition = query.get_condition()
    assert condition is not None
