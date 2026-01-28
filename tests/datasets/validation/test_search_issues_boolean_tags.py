from __future__ import annotations

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import reset_dataset_factory
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import TagConditionValidator

reset_dataset_factory()


def test_search_issues_boolean_tag_validation() -> None:
    """
    Test that boolean integer values in tag conditions work with search_issues entity.

    This simulates the real-world scenario from SNUBA-9VC where error.main_thread
    and stack.in_app fields are treated as tags with integer values.
    """
    entity = get_entity(EntityKey.SEARCH_ISSUES)

    # Query with error.main_thread:1 (boolean field as tag)
    query = LogicalQuery(
        QueryEntity(EntityKey.SEARCH_ISSUES, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("group_id", Column("_snuba_group_id", None, "group_id")),
        ],
        condition=binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 123),
            ),
            binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[error.main_thread]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "error.main_thread"),
                ),
                Literal(None, 1),  # Boolean integer value
            ),
        ),
    )

    # Validator should not raise an exception
    validator = TagConditionValidator()
    validator.validate(query)  # Should pass without error

    # Verify the literal was converted to string
    condition = query.get_condition()
    assert condition is not None


def test_search_issues_stack_in_app_tag() -> None:
    """
    Test the specific case from the error message: tags[stack.in_app]
    """
    entity = get_entity(EntityKey.SEARCH_ISSUES)

    query = LogicalQuery(
        QueryEntity(EntityKey.SEARCH_ISSUES, entity.get_data_model()),
        selected_columns=[
            SelectedExpression("group_id", Column("_snuba_group_id", None, "group_id")),
        ],
        condition=binary_condition(
            "and",
            binary_condition(
                "equals",
                Column("_snuba_project_id", None, "project_id"),
                Literal(None, 456),
            ),
            binary_condition(
                "equals",
                SubscriptableReference(
                    "_snuba_tags[stack.in_app]",
                    Column("_snuba_tags", None, "tags"),
                    Literal(None, "stack.in_app"),
                ),
                Literal(None, 1),  # The problematic integer from SNUBA-9VC
            ),
        ),
    )

    validator = TagConditionValidator()
    validator.validate(query)  # Should pass after fix
