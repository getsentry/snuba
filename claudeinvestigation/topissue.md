# Root Cause Analysis: SNUBA-9VC and SNUBA-9VD

## Executive Summary

**Issue IDs:** SNUBA-9VC, SNUBA-9VD
**Severity:** HIGH
**Status:** Unresolved
**First Seen:** 2025-10-29
**Combined Occurrences:** 62,482 events
**Primary Error:** `Validation failed for entity transactions: invalid tag condition on 'tags[issue.id]': 6868442908 must be a string`
**Secondary Error:** `Validation failed for entity group_attributes: missing required conditions for project_id`

These issues represent validation failures occurring when Sentry's frontend/backend sends queries to Snuba with improperly typed tag conditions.

---

## Issue Overview

### SNUBA-9VC
- **Description:** Invalid tag condition on 'tags[issue.id]' with integer value
- **Culprit:** `snql_dataset_query_view__transactions__api.discover.query-table`
- **Occurrences:** 32,797
- **Environment Distribution:** US (29,122), DE (3,720), and 32,840 other environments
- **URL:** https://sentry.sentry.io/issues/SNUBA-9VC

### SNUBA-9VD
- **Description:** Same error as SNUBA-9VC
- **Culprit:** `snql_dataset_query_view__transactions__api.discover.default-chart`
- **Occurrences:** 29,685
- **Environment Distribution:** Similar to SNUBA-9VC
- **URL:** https://sentry.sentry.io/issues/SNUBA-9VD

---

## Root Cause Analysis

### The Core Problem

Sentry is sending SnQL queries to Snuba that include tag conditions with **integer values instead of string values**. Specifically:

```snql
tags[issue.id] = 6868442908  -- WRONG: integer value
```

Should be:

```snql
tags[issue.id] = '6868442908'  -- CORRECT: string value
```

### Why This Validation Exists

The `TagConditionValidator` was introduced in **February 2023** (commit c12819028) to prevent common errors where Sentry sends improperly typed tag conditions to Snuba. From the original PR description:

> "Validate that simple tag conditions (conditions with a tags column on the left side and a literal or array of literals on the right side) are comparing against strings and nothing else. This is a common error that comes from Sentry, and other current checks don't catch it so it gets sent to Clickhouse and reported to Sentry as an error."

Tags in Snuba's data model are **always strings**. The validation exists to catch these type mismatches early and provide clear error messages rather than letting them propagate to ClickHouse where they cause cryptic failures.

---

## Technical Deep Dive

### Validation Code Location

**File:** `/workspace/snuba/query/validation/validators.py`
**Class:** `TagConditionValidator` (lines 316-364)

The validator checks every tag condition in a query:

```python
class TagConditionValidator(QueryValidator):
    """
    Sometimes a query will have a condition that compares a tag value to a
    non-string literal. This will always fail (tags are always strings) so
    catch this specific case and reject it.
    """

    def validate(self, query: Query, alias: Optional[str] = None) -> None:
        condition = query.get_condition()
        if not condition:
            return
        for cond in condition:
            match = self.condition_matcher.match(cond)
            if match:
                column = match.expression("column")
                col_str: str
                if not isinstance(column, SubscriptableReferenceExpr):
                    return  # only fail things on the tags[] column

                col_str = f"{column.column.column_name}[{column.key.value}]"
                error_prefix = f"invalid tag condition on '{col_str}':"

                rhs = match.expression("rhs")
                if isinstance(rhs, Literal):
                    if not isinstance(rhs.value, str):
                        raise InvalidQueryException(f"{error_prefix} {rhs.value} must be a string")
                elif isinstance(rhs, FunctionCall):
                    # The rhs is guaranteed to be an array function because of the match
                    for param in rhs.parameters:
                        if isinstance(param, Literal) and not isinstance(param.value, str):
                            raise InvalidQueryException(
                                f"{error_prefix} array literal {param.value} must be a string"
                            )
```

### Entities Using This Validator

The `TagConditionValidator` is configured for multiple entities in `/workspace/snuba/datasets/configuration/`:

1. **transactions/entities/transactions.yaml** (line 379)
2. **discover/entities/discover.yaml** (line 487)
3. **discover/entities/discover_transactions.yaml** (line 353)
4. **discover/entities/discover_events.yaml** (line 533)
5. **events/entities/events.yaml** (line 528)
6. **spans/entities/spans.yaml** (line 168)
7. **issues/entities/search_issues.yaml** (line 202)

### Error Flow

1. **Sentry Frontend/Backend** generates a query with `tags[issue.id] = 6868442908`
2. **Query Reception** at Snuba's API endpoint (`/transactions/snql`, `/discover/snql`)
3. **Query Parsing** converts the request to an internal query AST
4. **Entity Validation** runs via `run_entity_validators()` in `/workspace/snuba/datasets/plans/entity_validation.py`
5. **TagConditionValidator.validate()** detects the integer literal `6868442908`
6. **Exception Raised**: `InvalidQueryException` wrapped in `ValidationException`
7. **Error Response** sent back to Sentry: `Validation failed for entity transactions: invalid tag condition on 'tags[issue.id]': 6868442908 must be a string`

---

## Secondary Issue: group_attributes validation

In the same event stream, we also see:

```
Validation failed for entity group_attributes: missing required conditions for project_id
```

**Location:** `/workspace/snuba/datasets/configuration/group_attributes/entities/group_attributes.yaml` (lines 44-46)

```yaml
validators:
  - validator: EntityRequiredColumnValidator
    args:
      required_filter_columns: ["project_id"]
```

The `group_attributes` entity requires `project_id` as a mandatory filter condition (defined by `EntityRequiredColumnValidator` in `/workspace/snuba/query/validation/validators.py` lines 88-126). Queries hitting this entity are missing the required `project_id = <value>` condition in their WHERE clause.

---

## Why This Started in October 2025

The validator has been in place since February 2023, so the validation logic itself didn't change. What likely changed is:

1. **Sentry Product Changes:** A change in Sentry's codebase (likely in the dashboard/widget query building logic) started sending `issue.id` as an integer instead of a string
2. **New Feature:** A new feature or query pattern that references `tags[issue.id]` was introduced
3. **Query Builder Bug:** The query builder in Sentry's API layer is not properly stringifying issue IDs before including them in tag conditions

### Evidence

- The error consistently shows the same issue ID: `6868442908`
- It's hitting dashboard-related endpoints: `api.dashboards.tablewidget`, `api.discover.query-table`, `api.discover.default-chart`
- The high volume (62K+ events) suggests this is a widely-used query pattern, not a one-off bug

---

## Impact Assessment

### Severity: HIGH

- **User Impact:** Dashboard widgets and discover queries failing for users
- **Frequency:** 62,482 occurrences since October 29, 2025
- **Rate:** Ongoing (last seen January 27, 2026)
- **Scope:** Multiple Sentry environments (primarily US and DE regions)

### Affected Components

1. **Discover Query Tables** - Transaction data queries failing
2. **Dashboard Table Widgets** - Events data queries failing
3. **Default Charts** - Visualization queries failing
4. **Organization Events Meta** - Metadata queries failing

### Business Impact

- Users cannot view transaction data filtered by issue ID
- Dashboards showing errors instead of data
- Discovery queries failing silently or with error messages
- Potential loss of trust in the platform's reliability

---

## Code Locations Involved

### Primary Validation Logic
- **File:** `/workspace/snuba/query/validation/validators.py`
- **Lines:** 316-364 (`TagConditionValidator` class)

### Entity Configurations
- **Transactions:** `/workspace/snuba/datasets/configuration/transactions/entities/transactions.yaml:379`
- **Discover:** `/workspace/snuba/datasets/configuration/discover/entities/discover.yaml:487`
- **Events:** `/workspace/snuba/datasets/configuration/events/entities/events.yaml:528`
- **Group Attributes:** `/workspace/snuba/datasets/configuration/group_attributes/entities/group_attributes.yaml:44-46`

### Validation Execution
- **File:** `/workspace/snuba/datasets/plans/entity_validation.py`
- **Function:** `run_entity_validators()` (lines 65-84)
- **Function:** `_validate_entities_with_query()` (lines 31-59)

### Test Coverage
- **File:** `/workspace/tests/datasets/validation/test_tag_condition_checker.py`
- **Lines:** 25-142 (comprehensive test cases for tag validation)

---

## Stack Trace Analysis

Based on the Sentry event data:

```
Event: 301b40c5035c4cafadc93a1bc53e8972
Transaction: snql_dataset_query_view__transactions__api.discover.query-table
Timestamp: 2026-01-27T06:37:10.573Z

Error Message:
Validation failed for entity transactions: invalid tag condition on 'tags[issue.id]': 6868442908 must be a string

Trace Context:
- trace_id: 2897a9f571cd4baea80f36110491272b
- span_id: b9688294beefa827
- parent_span_id: 8697eb786cfaf7c5
- operation: function
- description: run_query
- thread: Dummy-12677

Request:
- Method: POST
- URL: http://snuba-api/transactions/snql
- Server: snuba-api-production-77bd9f4b4f-tl2pj
```

The error occurs during query execution in the validation phase, specifically when processing SnQL queries for the transactions entity.

---

## Why This Is Happening

### The Upstream Problem (Sentry)

Sentry's query builder is constructing SnQL queries with issue IDs as integers:

```python
# What Sentry is likely doing (WRONG):
issue_id = 6868442908  # integer from database
query = f"... WHERE tags[issue.id] = {issue_id}"  # No quotes!
```

Should be:

```python
# What Sentry should be doing (CORRECT):
issue_id = 6868442908
query = f"... WHERE tags[issue.id] = '{issue_id}'"  # Properly stringified
```

### Why Tags Must Be Strings

In Snuba's data model, tags are stored as string key-value pairs in ClickHouse. The schema for tags columns uses the `Array(String)` or `Map(String, String)` type. Comparing a string column to an integer is a type mismatch that ClickHouse would reject anyway, but with a less clear error message.

---

## Recommended Immediate Actions

### 1. Fix in Sentry (Upstream)
**Priority:** CRITICAL
**Owner:** Sentry product team

The issue must be fixed in Sentry's codebase where queries are constructed. Specifically:

- Locate the dashboard/widget query builder that references `tags[issue.id]`
- Ensure all tag values are properly stringified before being included in SnQL queries
- Add type checking to prevent integers from being passed as tag values

### 2. Add Monitoring (Snuba)
**Priority:** HIGH
**Owner:** Snuba team

Add metrics to track:
- Frequency of `TagConditionValidator` failures by tag key
- Which entities are most affected
- Which Sentry referrers are generating the invalid queries

```python
# In TagConditionValidator.validate()
metrics.increment(
    "tag_condition_validation_failure",
    tags={
        "entity": query.get_from_clause().key.value,
        "tag_key": column.key.value,
        "value_type": type(rhs.value).__name__
    }
)
```

### 3. Enhanced Error Messages (Snuba)
**Priority:** MEDIUM
**Owner:** Snuba team

Improve the error message to help Sentry developers debug faster:

```python
raise InvalidQueryException(
    f"{error_prefix} {rhs.value} must be a string. "
    f"Hint: Ensure tag values are quoted in your query. "
    f"Common cause: issue IDs being passed as integers instead of strings."
)
```

### 4. Add Query Validation in Sentry (Upstream)
**Priority:** MEDIUM
**Owner:** Sentry product team

Add client-side validation before queries are sent to Snuba:
- Validate tag condition types before query execution
- Add warnings/errors in the UI when malformed queries are detected
- Add integration tests that catch these type errors

---

## Long-term Solutions

### 1. Type-Aware Query Builder
Build a strongly-typed query builder in Sentry that prevents type mismatches at compile/build time.

### 2. Schema Validation API
Expose a schema validation endpoint in Snuba that Sentry can call before executing queries to validate query structure.

### 3. Automatic Type Coercion (Consider Carefully)
**Note:** This is controversial and may hide bugs rather than fix them.

Snuba could automatically coerce integer literals to strings in tag conditions:

```python
# In TagConditionValidator or a preprocessor
if isinstance(rhs, Literal) and isinstance(rhs.value, int):
    # Auto-coerce to string with a warning
    logger.warning(f"Auto-coercing integer {rhs.value} to string for tag condition")
    rhs.value = str(rhs.value)
```

However, this approach:
- Hides bugs in upstream code
- May mask other type-related issues
- Could impact performance if done on every query

---

## Test Cases

Existing test coverage in `/workspace/tests/datasets/validation/test_tag_condition_checker.py`:

```python
# Test case for integer tag value (should fail)
pytest.param(
    LogicalQuery(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[...],
        condition=binary_condition(
            "equals",
            SubscriptableReference(
                "_snuba_tags[count]",
                Column("_snuba_tags", None, "tags"),
                Literal(None, "count"),
            ),
            Literal(None, 419),  # INTEGER VALUE - WRONG
        ),
    ),
    InvalidQueryException("invalid tag condition on 'tags[count]': 419 must be a string"),
    id="comparing to non-string literal fails",
)
```

This test validates that the `TagConditionValidator` correctly rejects integer tag values.

---

## Related Issues

This issue is similar to historical issues where Sentry has sent improperly typed queries to Snuba:

1. **Tag count validation** - Same pattern with `tags[count]`
2. **DateTime condition validation** - Similar type checking for datetime columns
3. **Column existence validation** - Validates that referenced columns exist

The pattern suggests that Sentry's query building logic needs better type safety overall.

---

## Conclusion

**Root Cause:** Sentry is sending SnQL queries with `tags[issue.id]` conditions using integer values instead of string values.

**Why It Matters:** Tags in Snuba are always strings. Type mismatches cause query failures.

**Solution:** Fix Sentry's query builder to properly stringify issue IDs and other tag values before constructing SnQL queries.

**Severity:** HIGH - Affecting 62K+ queries across multiple dashboard and discover components.

**Action Required:** Coordinate with Sentry product team to fix the query building logic in the Sentry codebase.

---

## Appendix: Query Example

**Invalid Query (Current):**
```snql
MATCH (transactions)
SELECT count()
WHERE project_id IN [1234]
  AND tags[issue.id] = 6868442908  -- ERROR: integer value
  AND timestamp >= '2025-10-29'
```

**Valid Query (Expected):**
```snql
MATCH (transactions)
SELECT count()
WHERE project_id IN [1234]
  AND tags[issue.id] = '6868442908'  -- CORRECT: string value
  AND timestamp >= '2025-10-29'
```

---

**Report Generated:** 2026-01-27
**Analysis Tool:** Claude Code
**Analyst:** AI-powered root cause analysis
