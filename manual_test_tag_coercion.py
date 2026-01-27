#!/usr/bin/env python
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import reset_dataset_factory
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal, SubscriptableReference
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import TagConditionValidator

reset_dataset_factory()

# Create a query with integer tag value (like Sentry sends)
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

print("Before validation:")
print(f"Literal value: {literal_int.value}")
print(f"Literal type: {type(literal_int.value)}")

# Validate and auto-coerce
validator = TagConditionValidator()
try:
    validator.validate(query)
    print("\n✅ Validation succeeded!")
except Exception as e:
    print(f"\n❌ Validation failed: {e}")
    exit(1)

print("\nAfter validation:")
print(f"Literal value: {literal_int.value}")
print(f"Literal type: {type(literal_int.value)}")
print(f"Coerced correctly: {literal_int.value == '6868442908' and isinstance(literal_int.value, str)}")
