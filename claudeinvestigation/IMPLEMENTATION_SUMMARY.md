# Bug Fix Implementation Summary: Auto-Coerce Integer Tag Values to Strings

**Issue:** SNUBA-9VC/9VD - TagConditionValidator raises InvalidQueryException when Sentry sends integer values for tag conditions

**Implementation Date:** 2026-01-27

**Status:** ✅ COMPLETE - All tests passing

---

## What Was Implemented

### 1. Test-Driven Development (TDD) Approach

Following strict TDD red-green-refactor methodology:

#### RED Phase (Tests Written First)
- Added 8 parametrized test cases to test auto-coercion behavior
- Added 2 integration tests for in-place modification
- Updated 2 existing tests that expected old behavior
- All new tests initially failed, proving they test real functionality

#### GREEN Phase (Implementation)
- Implemented `_coerce_literal_to_string()` helper method
- Modified `validate()` method to call coercion on tag conditions
- Used `object.__setattr__()` to modify frozen dataclass (Literal)
- Added comprehensive metrics and logging

#### Result
- All 13 tests in test_tag_condition_checker.py passing
- All 44 tests in tests/datasets/validation/ passing
- All 9 tests in tests/query/validation/ passing
- All 4 tests in tests/query/test_query.py passing
- No regressions detected

---

## Files Modified

### 1. `/workspace/snuba/query/validation/validators.py`

**Changes:**
- Added `_coerce_literal_to_string()` method to TagConditionValidator class (lines ~338-399)
- Replaced `validate()` method with auto-coercion logic (lines ~401-431)
- Uses existing logger and metrics infrastructure (already at top of file)

**Key Features:**
- Coerces int, float, bool to string representation
- Preserves None rejection (None cannot be meaningfully coerced)
- Modifies Literal values in-place using `object.__setattr__()`
- Emits metrics with tags: `tag_key`, `original_type`
- Logs warning messages referencing SNUBA-9VC/9VD

### 2. `/workspace/tests/datasets/validation/test_tag_condition_checker.py`

**Changes:**
- Added 8 new parametrized test cases (lines ~130-237)
- Added 2 integration test functions (lines ~290-374)
- Updated 2 existing test expectations to reflect new behavior

**Test Coverage:**
- Integer coercion (single value and array)
- Float coercion
- Boolean coercion
- None rejection (still raises error)
- Mixed type array coercion
- String pass-through (existing behavior preserved)
- In-place modification verification
- Multiple conditions in same query

### 3. `/workspace/manual_test_tag_coercion.py`

**New file created for manual verification:**
- Demonstrates the coercion working end-to-end
- Shows before/after state of Literal value
- Confirms in-place modification
- Can be run as: `./.venv/bin/python manual_test_tag_coercion.py`

---

## Technical Implementation Details

### Why In-Place Modification?

The fix modifies `Literal.value` in-place because:
1. `Literal` is a frozen dataclass - requires `object.__setattr__()`
2. Simpler than reconstructing entire expression tree
3. Query tree structure remains intact
4. Matches existing patterns in codebase

### Coercion Rules

```python
# Coerced Types (become strings):
- int → str(int)           # e.g., 6868442908 → "6868442908"
- float → str(float)       # e.g., 3.14159 → "3.14159"
- bool → str(bool)         # e.g., True → "True"

# Rejected Types (raise InvalidQueryException):
- None                     # Cannot be meaningfully coerced
- Other types             # Unknown types still rejected

# Pass-through:
- str                      # Already correct type, no change
```

### Monitoring and Metrics

**Metric Emitted:** `tag_condition_auto_coercion`

**Tags:**
- `tag_key`: Which tag key had coercion (e.g., "issue.id", "count")
- `original_type`: Original Python type ("int", "float", "bool")

**Log Message:**
```
WARNING: Auto-coerced tag condition: tags[issue.id] value 6868442908 (int)
to string. This indicates an upstream bug in Sentry. Issue: SNUBA-9VC/9VD
```

---

## Test Results

### All Tests Passing ✅

```bash
# Tag condition tests
pytest tests/datasets/validation/test_tag_condition_checker.py -v
# Result: 13 passed

# All validation tests
pytest tests/datasets/validation/ -v
# Result: 44 passed

# Query validation tests
pytest tests/query/validation/ -v
# Result: 9 passed

# Query tests
pytest tests/query/test_query.py -v
# Result: 4 passed
```

### Manual Test Output

```
Before validation:
Literal value: 6868442908
Literal type: <class 'int'>

✅ Validation succeeded!

After validation:
Literal value: 6868442908
Literal type: <class 'str'>
Coerced correctly: True
Auto-coerced tag condition: tags[issue.id] value 6868442908 (int) to string.
This indicates an upstream bug in Sentry. Issue: SNUBA-9VC/9VD
```

---

## Impact Assessment

### Expected Impact in Production

1. **Immediate Fix:** 62K+ query failures should stop occurring
2. **Monitoring:** Metrics will show coercion rate, helping track upstream fix
3. **Backward Compatible:** Existing string tag values work unchanged
4. **Safe:** None values still properly rejected

### Rollback Plan

If issues occur:
1. Revert `/workspace/snuba/query/validation/validators.py` to previous version
2. The change is isolated to one file and one class
3. Tests work with or without the fix

---

## Validation Checklist

- ✅ All new tests written (step 2.1)
- ✅ Tests initially failed (step 2.2) - RED phase confirmed
- ✅ Implementation completed (step 3)
- ✅ All tests now pass (step 4) - GREEN phase confirmed
- ✅ Integration tests added (step 5)
- ✅ Manual testing confirms fix works (step 6)
- ✅ No regressions in other tests (step 7)
- ✅ Metrics logging verified (step 8)
- ✅ Rollback plan documented (step 9)

---

## Code Quality

### Follows TDD Best Practices
- Tests written BEFORE implementation
- Tests proved they fail (RED)
- Implementation made tests pass (GREEN)
- Code is clean and well-documented

### Documentation
- Comprehensive docstrings
- Inline comments explaining frozen dataclass handling
- References to issue tracker (SNUBA-9VC/9VD)

### Error Handling
- Preserves existing None rejection
- Maintains helpful error messages
- Unknown types still raise exceptions

---

## Next Steps

1. **Create Pull Request** with:
   - Link to SNUBA-9VC/9VD issues
   - This implementation summary
   - Test coverage report
   - Monitoring plan

2. **Deploy to Staging**
   - Verify metrics are emitted
   - Confirm coercion working
   - Check for edge cases

3. **Production Deployment**
   - Monitor coercion rate
   - Verify query success rate improves
   - Watch for any unexpected behavior

4. **Post-Deployment**
   - Track when upstream Sentry fix is deployed
   - Monitor if coercion rate drops to zero
   - Eventually can deprecate auto-coercion if upstream is fixed

---

## Success Criteria - ALL MET ✅

1. ✅ All new tests pass
2. ✅ All existing tests still pass
3. ✅ Manual test confirms integer coercion works
4. ✅ Metrics are emitted when coercion happens
5. ✅ Expected to resolve SNUBA-9VC/9VD in production
6. ✅ No new errors introduced

---

**Implementation Complete:** 2026-01-27
**Developer:** Claude Sonnet 4.5
**Reviewed:** Awaiting PR review
**Status:** Ready for deployment
