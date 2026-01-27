# BUGFIX IMPLEMENTATION PLAN: Auto-Coerce Integer Tag Values to Strings

**Issue:** SNUBA-9VC/9VD - TagConditionValidator raises InvalidQueryException when Sentry sends integer values for tag conditions
**Root Cause:** Upstream Sentry bug sends `tags[issue.id] = 6868442908` (int) instead of `tags[issue.id] = '6868442908'` (string)
**Impact:** 62K+ query failures affecting dashboards and discover queries
**Solution:** Pragmatic fix - auto-coerce numeric literals to strings with monitoring

---

## STEP 1: Understand Current Implementation

### Files to Review:
- `/workspace/snuba/query/validation/validators.py` (lines 316-365)
- `/workspace/tests/datasets/validation/test_tag_condition_checker.py`
- `/workspace/snuba/query/expressions.py` (Literal class at line 307)

### Current Behavior:
The `TagConditionValidator.validate()` method:
1. Iterates through all conditions in the query
2. Matches conditions on `tags[*]` subscriptable references
3. For single value conditions: checks if `rhs.value` is a string, raises exception if not
4. For array conditions (IN/NOT IN): checks each array element, raises exception if any non-string found

### Key Code Patterns:
```python
# Current rejection logic (line 356-357):
if not isinstance(rhs.value, str):
    raise InvalidQueryException(f"{error_prefix} {rhs.value} must be a string")

# Array rejection logic (line 361-364):
if isinstance(param, Literal) and not isinstance(param.value, str):
    raise InvalidQueryException(
        f"{error_prefix} array literal {param.value} must be a string"
    )
```

---

## STEP 2: Implement Auto-Coercion Logic (TDD Approach)

### 2.1 Write Failing Tests FIRST

**File:** `/workspace/tests/datasets/validation/test_tag_condition_checker.py`

Add these test cases to the `tests` list (after line 130, before the existing test cases):

```python
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
```

### 2.2 Run Tests to Confirm Failures

```bash
cd /workspace
python -m pytest tests/datasets/validation/test_tag_condition_checker.py -v
```

Expected: New test cases should fail with InvalidQueryException errors.

---

## STEP 3: Modify TagConditionValidator Implementation

**File:** `/workspace/snuba/query/validation/validators.py`

### 3.1 Add Helper Method for Coercion

Add this method to the TagConditionValidator class (after __init__, before validate):

```python
def _coerce_literal_to_string(
    self, literal: Literal, col_str: str, is_array_element: bool = False
) -> None:
    """
    Coerce a non-string literal to a string for tag conditions IN PLACE.

    Args:
        literal: The Literal expression to potentially coerce (modified in place)
        col_str: The column string for error messages (e.g., 'tags[issue.id]')
        is_array_element: Whether this literal is inside an array

    Raises:
        InvalidQueryException: If the value is None or cannot be coerced
    """
    if isinstance(literal.value, str):
        # Already a string, no coercion needed
        return

    if literal.value is None:
        # None cannot be coerced to a meaningful string
        error_prefix = f"invalid tag condition on '{col_str}':"
        if is_array_element:
            raise InvalidQueryException(
                f"{error_prefix} array literal None must be a string"
            )
        else:
            raise InvalidQueryException(
                f"{error_prefix} None must be a string"
            )

    # Coerce int, float, bool to string
    if isinstance(literal.value, (int, float, bool)):
        original_value = literal.value
        original_type = type(literal.value).__name__

        # Replace the value with string representation
        literal.value = str(literal.value)

        # Log metrics for monitoring
        metrics.increment(
            "tag_condition_auto_coercion",
            tags={
                "tag_key": col_str.split('[')[1].rstrip(']'),
                "original_type": original_type,
            },
        )
        logger.warning(
            f"Auto-coerced tag condition: {col_str} "
            f"value {original_value} ({original_type}) to string. "
            f"This indicates an upstream bug in Sentry. Issue: SNUBA-9VC/9VD"
        )
        return

    # Unknown type - keep existing behavior (raise exception)
    error_prefix = f"invalid tag condition on '{col_str}':"
    if is_array_element:
        raise InvalidQueryException(
            f"{error_prefix} array literal {literal.value} must be a string"
        )
    else:
        raise InvalidQueryException(
            f"{error_prefix} {literal.value} must be a string"
        )
```

### 3.2 Modify validate() Method to Use Auto-Coercion

Replace the validate method (lines 339-364) with:

```python
def validate(self, query: Query, alias: Optional[str] = None) -> None:
    """
    Validate tag conditions and auto-coerce numeric literals to strings.

    IMPORTANT: This auto-coercion is a pragmatic fix for upstream Sentry bugs
    (SNUBA-9VC/9VD). Ideally, Sentry should send properly typed tag values.
    The metrics and logging help us monitor when this happens and validate
    when the upstream fix is deployed.
    """
    condition = query.get_condition()
    if not condition:
        return

    for cond in condition:
        match = self.condition_matcher.match(cond)
        if match:
            column = match.expression("column")
            if not isinstance(column, SubscriptableReferenceExpr):
                continue  # only process things on the tags[] column

            col_str = f"{column.column.column_name}[{column.key.value}]"

            rhs = match.expression("rhs")
            if isinstance(rhs, Literal):
                # Single literal value - coerce if needed
                self._coerce_literal_to_string(rhs, col_str, is_array_element=False)
            elif isinstance(rhs, FunctionCall):
                # Array function - coerce each parameter if needed
                for param in rhs.parameters:
                    if isinstance(param, Literal):
                        self._coerce_literal_to_string(param, col_str, is_array_element=True)
```

### 3.3 Ensure Required Imports (top of file)

Verify these imports exist at the top of `/workspace/snuba/query/validation/validators.py`:

```python
import logging
from snuba import settings
from snuba.utils.metrics.wrapper import MetricsWrapper

# Add at top if not present
logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "api")
```

---

## STEP 4: Run Tests to Verify Fix

```bash
cd /workspace
python -m pytest tests/datasets/validation/test_tag_condition_checker.py -v
```

Expected: All tests should now pass, including the new coercion tests.

---

## STEP 5: Add Integration Tests

**File:** `/workspace/tests/datasets/validation/test_tag_condition_checker.py`

Add these test functions at the end of the file (after the parametrized tests):

```python
def test_tag_condition_coercion_modifies_query_in_place():
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


def test_tag_condition_coercion_with_multiple_types():
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
```

---

## STEP 6: Manual Testing

Create a test script to verify the fix works with real queries:

**File:** `/workspace/manual_test_tag_coercion.py`

```python
#!/usr/bin/env python
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, Literal, SubscriptableReference
from snuba.query.logical import Query as LogicalQuery
from snuba.query.validation.validators import TagConditionValidator

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
```

Run:
```bash
cd /workspace
python manual_test_tag_coercion.py
```

Expected output:
```
Before validation:
Literal value: 6868442908
Literal type: <class 'int'>

✅ Validation succeeded!

After validation:
Literal value: 6868442908
Literal type: <class 'str'>
Coerced correctly: True
```

---

## STEP 7: Verify No Regressions

Run all related tests to ensure no regressions:

```bash
cd /workspace
# Run all validation tests
python -m pytest tests/datasets/validation/ -v

# Run query validation tests
python -m pytest tests/query/validation/ -v

# Run query tests
python -m pytest tests/query/test_query.py -v
```

All tests should pass.

---

## STEP 8: Monitoring and Metrics

### Metrics Emitted

The fix emits this metric when auto-coercion happens:

**Metric:** `tag_condition_auto_coercion`
**Tags:**
- `tag_key`: Which tag key had the coercion (e.g., "issue.id", "count")
- `original_type`: The original type that was coerced ("int", "float", "bool")

### Monitoring Queries

```promql
# Rate of tag coercions per minute
rate(tag_condition_auto_coercion[1m])

# Coercions by tag key
sum by (tag_key) (tag_condition_auto_coercion)

# Coercions by type
sum by (original_type) (tag_condition_auto_coercion)
```

### Alerts to Set Up

1. **High Coercion Rate**: Alert if coercion rate > 1000/min
2. **New Tag Keys**: Alert on new tag_key values appearing
3. **Trend Changes**: Alert on 50% increase in coercion rate week-over-week

---

## STEP 9: Rollback Plan

If issues occur after deployment:

1. **Immediate Rollback**: Revert validators.py to previous version
2. **Partial Rollback**: Remove the coercion logic from validate() method but keep the helper method
3. **Test Rollback**: The existing tests will work with or without this fix

The change is isolated to one file and one class, making rollback straightforward.

---

## VALIDATION CHECKLIST

Before considering this complete:

- [ ] All new tests written (step 2.1)
- [ ] Tests initially fail (step 2.2)
- [ ] Implementation completed (step 3)
- [ ] All tests now pass (step 4)
- [ ] Integration tests added (step 5)
- [ ] Manual testing confirms fix works (step 6)
- [ ] No regressions in other tests (step 7)
- [ ] Metrics logging verified (step 8)
- [ ] Rollback plan documented (step 9)

---

## FILES TO MODIFY

1. **Primary:** `/workspace/snuba/query/validation/validators.py`
   - Add `_coerce_literal_to_string()` method to TagConditionValidator
   - Modify `validate()` method to call coercion helper
   - Add metrics and logging

2. **Tests:** `/workspace/tests/datasets/validation/test_tag_condition_checker.py`
   - Add 8 parametrized test cases
   - Add 2 integration test functions

3. **Manual Test:** `/workspace/manual_test_tag_coercion.py`
   - Create new file for manual verification

---

## IMPORTANT NOTES

### Why In-Place Mutation?

The fix modifies Literal.value in place because:
1. Literal objects are simple value containers
2. The query tree structure remains intact
3. Simpler than reconstructing the entire expression tree
4. Matches existing patterns in the codebase (see type_converters.py)

### Why Not Use a Processor?

Validators run before processors in the pipeline. This is the right place because:
1. Other validators may depend on correct types
2. Processors expect validated queries
3. Keeps the fix isolated to one place

### Backward Compatibility

- Existing string tag values: No change
- None values: Still raise errors (correct behavior)
- Non-tag subscriptables: Not affected
- Complex expressions: Pass through unchanged

---

## TIMELINE ESTIMATE

- Step 1 (Understanding): 30 minutes
- Step 2 (Write tests): 45 minutes
- Step 3 (Implementation): 60 minutes
- Step 4-7 (Testing): 45 minutes
- Step 8-9 (Monitoring setup): 30 minutes

**Total:** ~3.5 hours for complete implementation and testing

---

## SUCCESS CRITERIA

1. All new tests pass
2. All existing tests still pass
3. Manual test confirms integer coercion works
4. Metrics are emitted when coercion happens
5. SNUBA-9VC/9VD error rate drops to zero in production
6. No new errors introduced

---

**Plan Version:** 1.0
**Created:** 2026-01-27
**Issue:** SNUBA-9VC/9VD
**Impact:** 62K+ query failures
