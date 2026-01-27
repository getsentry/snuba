# Pull Request: Fix SNUBA-9VC and SNUBA-9VD - Auto-coerce Numeric Tag Values to Strings

## Summary

This PR fixes issues SNUBA-9VC and SNUBA-9VD by automatically coercing integer, float, and boolean values to strings in tag conditions. This prevents 62,000+ validation failures that have been occurring since October 2025.

## Problem

Sentry is sending SnQL queries with numeric tag values instead of string values:

```snql
-- Current (WRONG):
tags[issue.id] = 6868442908  -- integer value

-- Expected (CORRECT):
tags[issue.id] = '6868442908'  -- string value
```

This causes `InvalidQueryException` errors because tags in ClickHouse are always strings. The issue affects:
- Dashboard table widgets
- Discover query tables
- Default charts and visualizations
- Multiple Sentry environments (primarily US and DE regions)

### Impact

- **62,482+ occurrences** since October 29, 2025
- **Still active** - last seen January 27, 2026
- **High priority** - Sentry actionability rating: HIGH
- **Customer-facing** - Affects dashboard widgets and discover queries

## Root Cause

The issue originates in Sentry's query building logic (upstream), which is not properly stringifying issue IDs before including them in tag conditions. However, this is a pragmatic fix in Snuba to unblock users while the upstream fix is developed.

## Solution

Modified `TagConditionValidator` in `/workspace/snuba/query/validation/validators.py` to:

1. **Auto-coerce** numeric types (int, float, bool) to strings
2. **Still reject** None/null values (cannot be meaningfully coerced)
3. **Emit metrics** for monitoring coercion frequency
4. **Log warnings** referencing SNUBA-9VC/9VD for tracking

### Implementation Details

**New Method: `_coerce_literal_to_string()`**
- Handles coercion of individual Literal values
- Uses `object.__setattr__()` to modify frozen dataclass
- Provides different error messages for single vs array elements
- Tracks metrics for each coercion

**Modified Method: `validate()`**
- Iterates through tag conditions
- Calls coercion helper on all tag literals
- Modifies values in-place (simpler than tree reconstruction)
- Maintains backward compatibility with existing string values

### Code Changes

```python
# Before (rejected with error):
if not isinstance(rhs.value, str):
    raise InvalidQueryException(f"{error_prefix} {rhs.value} must be a string")

# After (auto-coerce with monitoring):
self._coerce_literal_to_string(rhs, col_str, is_array_element=False)
```

## Testing

### Test Coverage

**Added 10 new tests:**
- ✅ Integer tag values auto-coerced
- ✅ Float tag values auto-coerced
- ✅ Boolean tag values auto-coerced
- ✅ None values still raise errors
- ✅ Array conditions with mixed types handled
- ✅ None in arrays still raises errors
- ✅ Existing string values work unchanged
- ✅ In-place modification verified
- ✅ Multiple types in same query

**Updated 2 existing tests:**
- Changed expectations from "should fail" to "should pass" for numeric types

### Test Results

```bash
tests/datasets/validation/test_tag_condition_checker.py: 13/13 passed ✅
tests/datasets/validation/: 44/44 passed ✅
tests/query/validation/: 9/9 passed ✅
tests/query/test_query.py: 4/4 passed ✅

Total: 57/57 tests passing - NO REGRESSIONS
```

### Manual Testing

Created `/workspace/manual_test_tag_coercion.py` to verify end-to-end:

```bash
$ python manual_test_tag_coercion.py
Before validation: 6868442908 (int)
✅ Validation succeeded!
After validation: "6868442908" (str)
Coerced correctly: True
```

## Monitoring

### Metrics Emitted

**Metric:** `tag_condition_auto_coercion`

**Tags:**
- `tag_key`: Which tag had coercion (e.g., "issue.id")
- `original_type`: Original type coerced ("int", "float", "bool")

### Recommended Monitoring Queries

```promql
# Rate of tag coercions per minute
rate(tag_condition_auto_coercion[1m])

# Coercions by tag key
sum by (tag_key) (tag_condition_auto_coercion)

# Coercions by type
sum by (original_type) (tag_condition_auto_coercion)
```

### Recommended Alerts

1. **High Coercion Rate**: Alert if rate > 1000/min
2. **New Tag Keys**: Alert on new `tag_key` values
3. **Trend Changes**: Alert on 50% increase week-over-week

## Files Changed

### Core Implementation
- **`snuba/query/validation/validators.py`** (+102, -16 lines)
  - Added `_coerce_literal_to_string()` helper method
  - Modified `validate()` to perform auto-coercion
  - Added metrics and logging

### Tests
- **`tests/datasets/validation/test_tag_condition_checker.py`** (+250, -2 lines)
  - Added 8 parametrized test cases
  - Added 2 integration test functions
  - Updated 2 existing test expectations

### Manual Testing
- **`manual_test_tag_coercion.py`** (+48 lines, new file)
  - Manual verification script
  - Demonstrates end-to-end coercion

## Backward Compatibility

✅ **Fully backward compatible:**
- Existing string tag values: No change
- None values: Still rejected (correct behavior)
- Complex expressions: Pass through unchanged
- Non-tag subscriptables: Not affected
- Other validators: No changes needed

## Rollback Plan

If issues occur:
1. Revert `validators.py` to previous version
2. The change is isolated to one class
3. Tests will work with or without this fix

## Why This Approach?

### Alternative 1: Reject and Wait for Sentry Fix
- ❌ Blocks 62K+ queries until upstream fix deployed
- ❌ Users see errors instead of data
- ❌ Could take weeks/months for upstream fix

### Alternative 2: Auto-coerce (This PR) ✅
- ✅ Unblocks users immediately
- ✅ Metrics help track when upstream fix is deployed
- ✅ Safe - coercion is lossless for numeric types
- ✅ Can be removed once upstream fixed

### Why In-Place Modification?

Using `object.__setattr__()` to modify frozen dataclass:
- Simpler than reconstructing entire expression tree
- Matches patterns elsewhere in codebase (see `type_converters.py`)
- Query structure remains intact
- More efficient than creating new objects

## Future Work

Once Sentry deploys their upstream fix:
1. Monitor metrics for 30 days
2. If coercion rate drops to zero, remove auto-coercion
3. Add comment noting when/why removed

## Related Issues

- **SNUBA-9VC** - 32,797 occurrences
- **SNUBA-9VD** - 29,685 occurrences
- Upstream Sentry issue: [needs to be filed]

## Validation Checklist

- [x] All new tests pass
- [x] All existing tests pass (no regressions)
- [x] Manual testing confirms fix works
- [x] Metrics/logging implemented
- [x] Documentation updated in code comments
- [x] Backward compatibility verified
- [x] Rollback plan documented

## Branch Information

- **Branch:** `fix/snuba-9vc-9vd-tag-coercion`
- **Base:** `master`
- **Commit:** `65e8ac111`

## How to Test This PR

### 1. Run automated tests
```bash
cd /workspace
python -m pytest tests/datasets/validation/test_tag_condition_checker.py -v
python -m pytest tests/datasets/validation/ -v
python -m pytest tests/query/validation/ -v
```

### 2. Run manual test
```bash
python manual_test_tag_coercion.py
```

### 3. Check for regressions
```bash
python -m pytest tests/ -k validation
```

## Deployment Steps

1. Merge this PR to master
2. Deploy to staging environment
3. Monitor `tag_condition_auto_coercion` metric
4. If metric shows expected coercion rate, deploy to production
5. Monitor production for 24 hours
6. Verify SNUBA-9VC/9VD error rates drop to zero
7. Set up alerts for coercion rate changes

## Success Criteria

- ✅ SNUBA-9VC/9VD error rate drops to zero
- ✅ Dashboard widgets and discover queries work
- ✅ No new errors introduced
- ✅ Metrics show coercion happening as expected
- ✅ No performance degradation

---

**Ready for Review** ✅

This PR is production-ready with comprehensive testing and no regressions detected. The fix is pragmatic, safe, and unblocks users immediately while allowing upstream Sentry fix to be developed at their pace.
