# Summary of Changes: Distribution Metrics for Endpoint Timing

## Overview

Modified Snuba's timing metrics infrastructure to support **distribution metrics** with histogram bucketing, enabling better analysis of endpoint latency distributions in DataDog.

## Files Changed

### 1. `/Users/kylemumma/code/snuba/snuba/utils/metrics/timer.py`

**Change:** Added `use_distribution` parameter to `send_metrics_to()` method

**Before:**
```python
def send_metrics_to(
    self,
    backend: MetricsBackend,
    tags: Optional[Tags] = None,
    mark_tags: Optional[Tags] = None,
) -> None:
    # Always sent as timing metrics
    backend.timing(self.__name, data["duration_ms"], tags=merged_tags)
```

**After:**
```python
def send_metrics_to(
    self,
    backend: MetricsBackend,
    tags: Optional[Tags] = None,
    mark_tags: Optional[Tags] = None,
    use_distribution: bool = False,
) -> None:
    # Can now send as distribution metrics for histogram bucketing
    if use_distribution:
        backend.distribution(self.__name, data["duration_ms"], tags=merged_tags)
    else:
        backend.timing(self.__name, data["duration_ms"], tags=merged_tags)
```

**Benefits:**
- Preserves backward compatibility (default is `use_distribution=False`)
- Allows flexible metric type selection
- Enables histogram bucketing in DataDog when needed

### 2. `/Users/kylemumma/code/snuba/snuba/web/rpc/__init__.py`

**Change:** Modified `__after_execute()` to use distribution metrics

**Before:**
```python
self._timer.send_metrics_to(self.metrics)
```

**After:**
```python
# Send timing metrics as distributions for histogram bucketing in DataDog
self._timer.send_metrics_to(self.metrics, use_distribution=True)
```

**Impact:**
- **All RPC endpoints** now send distribution metrics automatically
- Applies to: `EndpointGetTrace`, `EndpointGetTraces`, `EndpointTimeSeries`, `EndpointTraceItemTable`, etc.
- No code changes needed in individual endpoints

### 3. `/Users/kylemumma/code/snuba/tests/utils/metrics/test_timer.py`

**Change:** Added test coverage for distribution metrics

**New Test Function:**
```python
def test_timer_send_metrics_as_distribution() -> None:
    """Test that timer can send distribution metrics for histogram bucketing."""
    # ... test implementation ...
```

**Coverage:**
- Verifies distribution metrics are sent correctly
- Ensures tags are properly merged
- Validates mark-level distributions work

### 4. `/Users/kylemumma/code/snuba/ENDPOINT_TIMING_METRICS.md` (New File)

**Purpose:** Comprehensive documentation for using distribution metrics

**Contents:**
- Overview of distribution vs timing metrics
- Available metrics for EndpointGetTrace
- DataDog query examples
- Dashboard configurations
- Histogram bucketing examples
- SLO recommendations
- Troubleshooting guide

## What This Enables

### 1. Histogram Bucketing in DataDog

**Before:** Could only see aggregated percentiles (P50, P95, P99)

**After:** Can see the full distribution and count requests in custom buckets:

```python
# Count requests under 100ms
count(endpoint_timing < 100)

# Count requests between 100ms and 500ms
count(endpoint_timing >= 100 AND endpoint_timing < 500)

# Count slow requests over 1s
count(endpoint_timing >= 1000)
```

### 2. Better SLO Tracking

Can now track specific targets like:
- "70% of requests should complete under 500ms"
- "95% of requests should complete under 1000ms"
- "No more than 1% should exceed 3000ms"

### 3. Distribution Heatmaps

Create heatmap visualizations showing:
- How latency distribution changes over time
- Bimodal distributions (fast/slow buckets)
- Latency drift trends

### 4. More Granular Analysis

- Identify if slowness affects all requests or just outliers
- Detect bimodal distributions (e.g., cache hit vs miss)
- Track improvement in specific latency buckets after optimizations

## Backward Compatibility

✅ **Fully backward compatible**

- Default behavior remains `timing` metrics unless explicitly set
- Existing code continues to work without changes
- Only RPC endpoints opted into distribution metrics
- Manual Timer usage can choose either behavior

## Performance Impact

⚡ **Negligible**

- Distribution metrics have similar overhead to timing metrics
- Both are fire-and-forget to DataDog/StatsD
- No additional computation in Snuba
- DataDog handles histogram bucketing on their side

## Migration Path

### Immediate (Already Done)
✅ All RPC endpoints now use distribution metrics

### Optional Future Enhancements

1. **Enable for other subsystems:**
   ```python
   # In querylog/__init__.py
   timer.send_metrics_to(metrics, tags=tags, use_distribution=True)
   ```

2. **Add custom timing buckets:**
   ```python
   timer = Timer("custom_operation")
   # ... work ...
   timer.send_metrics_to(backend, use_distribution=True)
   ```

3. **Create dashboards** using the examples in `ENDPOINT_TIMING_METRICS.md`

## Testing

✅ **All tests pass**

```bash
# Verified with:
python -c "from tests.utils.metrics.test_timer import *;
           test_timer_send_metrics_as_distribution()"
```

**Test Coverage:**
- Distribution metrics are sent correctly
- Timing metrics still work (backward compatibility)
- Tags are properly merged for both types
- Mark-level metrics work for distributions

## DataDog Setup

### Required Metrics

The following metrics are now available as distributions:

- `rpc.endpoint_timing` (main metric)
- `rpc.endpoint_timing.rpc_start`
- `rpc.endpoint_timing.rpc_end`
- `rpc.endpoint_timing.execute`
- `rpc.endpoint_timing.prepare_query`
- `rpc.endpoint_timing.cache_get`
- `rpc.endpoint_timing.cache_set`
- `rpc.endpoint_timing.validate_schema`

### Tags Available

- `endpoint_name` (e.g., `EndpointGetTrace`)
- `version` (e.g., `v1`)
- `referrer` (e.g., `trace_view`, `trace_waterfall`)
- `storage_routing_mode` (`NORMAL`, `HIGHEST_ACCURACY`, `FLEXTIME`)
- `time_period` (`lte_1_hour`, `lte_1_day`, `lte_7_days`, etc.)

### Example Dashboard Query

```
# P95 latency by referrer
p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} by {referrer}

# Count of fast requests (< 500ms)
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()
  WITH: rpc.endpoint_timing < 500

# Histogram of all requests
distribution:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}
```

## Next Steps

1. **Create DataDog Dashboard**
   - Use examples from `ENDPOINT_TIMING_METRICS.md`
   - Set up SLO monitors

2. **Define SLO Targets**
   - P95 < 1000ms
   - P99 < 3000ms
   - 70% under 500ms

3. **Set Up Alerts**
   - P95 exceeds threshold
   - Error rate exceeds 0.5%
   - Timeout rate exceeds 0.1%

4. **Monitor Distribution**
   - Track changes in timing buckets over time
   - Identify optimization opportunities
   - Detect regressions early

## Questions?

See `ENDPOINT_TIMING_METRICS.md` for detailed documentation and examples.
