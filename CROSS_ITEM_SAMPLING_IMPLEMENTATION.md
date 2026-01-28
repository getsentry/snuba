# Cross-Item Query Sampling Implementation

## Overview
This implementation adds conditional sampling behavior for cross-item queries in `EndpointTimeSeries` and `EndpointTraceItemTable`. When trace filters are present, the outer query can optionally skip sampling while the inner query (getting trace IDs) still uses sampling.

## Changes Made

### 1. Runtime Configuration
- **Config Key**: `cross_item_queries_no_sample_outer`
- **Type**: Integer (0 = disabled, 1 = enabled)
- **Default**: 0 (disabled)
- **Purpose**: Controls whether to skip sampling on the outer query when trace_filters are present

### 2. Code Changes

#### File: `snuba/web/rpc/v1/resolvers/R_eap_items/resolver_time_series.py`
- **Line 25**: Added `from snuba import state` import
- **Lines 476-480**: Added runtime config check:
  ```python
  cross_item_queries_no_sample_outer = state.get_int_config(
      "cross_item_queries_no_sample_outer", 0
  )
  if not (in_msg.trace_filters and cross_item_queries_no_sample_outer):
      query_settings.set_sampling_tier(routing_decision.tier)
  ```

#### File: `snuba/web/rpc/v1/resolvers/R_eap_items/resolver_trace_item_table.py`
- **Line 27**: Added `from snuba import state` import
- **Lines 582-586**: Added runtime config check (same logic as TimeSeries)

### 3. Test Files Created

#### `tests/web/rpc/v1/test_endpoint_time_series/test_endpoint_time_series_cross_item_sampling.py`
Tests that verify:
1. When config is enabled, SQL contains both outer and inner queries
2. Trace ID subquery pattern is present
3. When config is disabled, queries work as expected

#### `tests/web/rpc/v1/test_endpoint_trace_item_table/test_endpoint_trace_item_table_cross_item_sampling.py`
Same test coverage for TraceItemTable endpoint

## How It Works

### Sampling Mechanism
In Snuba EAP, sampling is implemented by selecting different physical storage tables:
- **TIER_NO_TIER** or **TIER_1** → `EAP_ITEMS` (full data, no sampling)
- **TIER_8** → `EAP_ITEMS_DOWNSAMPLE_8` (1/8 sample rate)
- **TIER_64** → `EAP_ITEMS_DOWNSAMPLE_64` (1/64 sample rate)
- **TIER_512** → `EAP_ITEMS_DOWNSAMPLE_512` (1/512 sample rate)

This is controlled by `query_settings.set_sampling_tier()` which is read by `EAPItemsStorageSelector`.

### Before This Change
When trace_filters were present:
- Inner query (getting trace IDs): Uses `routing_decision.tier` → queries downsampled table
- Outer query: Uses `routing_decision.tier` → queries same downsampled table

### After This Change (when config enabled)
When trace_filters are present AND `cross_item_queries_no_sample_outer=1`:
- Inner query: Uses `routing_decision.tier` → queries downsampled table
- Outer query: Does NOT set sampling tier (stays at `TIER_NO_TIER`) → queries full `EAP_ITEMS` table

### SQL Structure
The generated SQL has this pattern:
```sql
SELECT ...
FROM eap_items_X_local  -- Outer query: full table when config enabled
WHERE trace_id IN (
    SELECT trace_id
    FROM eap_items_Y_local  -- Inner query: downsampled table
    WHERE <trace_filter_conditions>
    ...
)
```

## Usage

### Enable the Feature
```python
from snuba import state
state.set_config("cross_item_queries_no_sample_outer", 1)
```

### Disable the Feature
```python
from snuba import state
state.delete_config("cross_item_queries_no_sample_outer")
# Or set to 0
state.set_config("cross_item_queries_no_sample_outer", 0)
```

### Check Current Value
```python
from snuba import state
value = state.get_int_config("cross_item_queries_no_sample_outer", 0)
print(f"Config is {'enabled' if value else 'disabled'}")
```

## Testing

Run the new tests:
```bash
# TimeSeries endpoint tests
pytest tests/web/rpc/v1/test_endpoint_time_series/test_endpoint_time_series_cross_item_sampling.py -v

# TraceItemTable endpoint tests
pytest tests/web/rpc/v1/test_endpoint_trace_item_table/test_endpoint_trace_item_table_cross_item_sampling.py -v
```

## Benefits

1. **Accuracy**: Outer query gets data from full storage, improving accuracy of final results
2. **Performance**: Inner query still uses sampling for trace ID lookup, maintaining performance
3. **Flexibility**: Feature can be enabled/disabled via runtime config without code changes
4. **Safe Rollout**: Disabled by default, can be gradually enabled for testing

## Future Considerations

- Monitor query performance with config enabled vs disabled
- Consider making this behavior permanent if it proves beneficial
- May want to add metrics to track usage and performance impact
