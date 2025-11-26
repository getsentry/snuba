# EndpointGetTrace Timing Metrics Guide

This document explains how to track and analyze user-facing timing metrics for the `EndpointGetTrace` endpoint using distribution metrics and histogram bucketing.

## Overview

As of the latest changes, all RPC endpoint timing metrics are now sent as **distribution metrics** to DataDog, enabling histogram bucketing and more granular analysis of latency distributions.

## What Changed

### Before
- Timing metrics were sent using `backend.timing()`, which aggregates into average/min/max/percentiles
- Limited visibility into the actual distribution of timing values

### After
- Timing metrics are sent using `backend.distribution()`, which:
  - Preserves the actual distribution of values
  - Enables histogram bucketing in DataDog
  - Allows counting datapoints that fall into specific timing buckets
  - Still supports percentile calculations (P50, P75, P90, P95, P99)

## Key Metrics Available

### 1. Overall Endpoint Duration (Primary Metric)

**Metric Name:** `rpc.endpoint_timing`

**Description:** Total time from request received to response sent

**Tags:**
- `endpoint_name`: `EndpointGetTrace`
- `version`: `v1`
- `referrer`: The API referrer/caller (e.g., `trace_view`, `trace_waterfall`)
- `storage_routing_mode`: Storage routing mode (`NORMAL`, `HIGHEST_ACCURACY`, `FLEXTIME`)
- `time_period`: Query time window (`lte_1_hour`, `lte_1_day`, `lte_7_days`, etc.)

**Example DataDog Query:**
```
avg:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} by {referrer}
```

### 2. Stage-Level Timing Metrics

These metrics show time spent in specific stages of request processing:

- **`rpc.endpoint_timing.rpc_start`** - Time to start RPC execution
- **`rpc.endpoint_timing.rpc_end`** - Total RPC processing time
- **`rpc.endpoint_timing.execute`** - ClickHouse query execution time
- **`rpc.endpoint_timing.prepare_query`** - Query preparation/building time
- **`rpc.endpoint_timing.cache_get`** - Cache read time
- **`rpc.endpoint_timing.cache_set`** - Cache write time
- **`rpc.endpoint_timing.validate_schema`** - Request validation time

**Example DataDog Query (Stacked View):**
```
sum:rpc.endpoint_timing.prepare_query{endpoint_name:EndpointGetTrace}.as_count(),
sum:rpc.endpoint_timing.execute{endpoint_name:EndpointGetTrace}.as_count(),
sum:rpc.endpoint_timing.cache_get{endpoint_name:EndpointGetTrace}.as_count()
```

### 3. Success/Error Rate Metrics

- **`rpc.request_success`** - Successful requests (counter)
- **`rpc.request_error`** - Failed requests (counter)
- **`rpc.request_invalid`** - Invalid requests (counter)
- **`rpc.request_rate_limited`** - Rate-limited requests (counter)

**Example Error Rate Query:**
```
(sum:rpc.request_error{endpoint_name:EndpointGetTrace}.as_count() +
 sum:rpc.request_invalid{endpoint_name:EndpointGetTrace}.as_count()) /
(sum:rpc.request_success{endpoint_name:EndpointGetTrace}.as_count() +
 sum:rpc.request_error{endpoint_name:EndpointGetTrace}.as_count() +
 sum:rpc.request_invalid{endpoint_name:EndpointGetTrace}.as_count()) * 100
```

### 4. Specialized GetTrace Metrics

- **`rpc.eap_trace_request_without_limit`** - Requests without pagination
- **`rpc.timeout_query`** - Queries that timed out
- **`rpc.OOM_query`** - Out-of-memory queries
- **`rpc.estimated_execution_timeout`** - Queries hitting execution timeout

## Creating Histogram Buckets in DataDog

### Distribution Summary Widget

1. In DataDog, create a new widget
2. Select "Distribution" as the visualization type
3. Use the metric: `rpc.endpoint_timing`
4. Filter by: `endpoint_name:EndpointGetTrace`
5. Choose "Histogram" display mode

This will show you a histogram of request timing values with automatic bucketing.

### Custom Bucket Counts

To count requests in specific timing buckets:

**Example: Count requests under 100ms**
```
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()
WITH: rpc.endpoint_timing < 100
```

**Example: Count requests between 100ms and 500ms**
```
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()
WITH: rpc.endpoint_timing >= 100 AND rpc.endpoint_timing < 500
```

**Example: Count slow requests (over 1000ms)**
```
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()
WITH: rpc.endpoint_timing >= 1000
```

### Bucket Aggregation Query

Create custom buckets for SLO tracking:

```sql
-- In DataDog Query Editor
SELECT
  count_nonzero(endpoint_timing < 100) as "< 100ms",
  count_nonzero(endpoint_timing >= 100 AND endpoint_timing < 500) as "100-500ms",
  count_nonzero(endpoint_timing >= 500 AND endpoint_timing < 1000) as "500ms-1s",
  count_nonzero(endpoint_timing >= 1000 AND endpoint_timing < 3000) as "1s-3s",
  count_nonzero(endpoint_timing >= 3000) as "> 3s"
FROM rpc.endpoint_timing
WHERE endpoint_name = 'EndpointGetTrace'
```

## Recommended Dashboard Views

### 1. Latency Percentiles (SLO Monitoring)

**Widget Type:** Timeseries

**Metrics:**
- `p50:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
- `p75:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
- `p90:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
- `p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
- `p99:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`

**Group by:** `referrer`, `storage_routing_mode`

### 2. Request Volume by Timing Bucket

**Widget Type:** Stacked Area Chart

**Metrics:**
```
< 100ms:   count(endpoint_timing < 100)
100-500ms: count(endpoint_timing >= 100 AND endpoint_timing < 500)
500ms-1s:  count(endpoint_timing >= 500 AND endpoint_timing < 1000)
1s-3s:     count(endpoint_timing >= 1000 AND endpoint_timing < 3000)
> 3s:      count(endpoint_timing >= 3000)
```

### 3. Timing Distribution Heatmap

**Widget Type:** Heatmap

**Metric:** `rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`

This shows timing distribution over time with color intensity indicating request volume.

### 4. Error Rate vs Latency

**Widget Type:** Graph with dual Y-axes

**Left Y-axis:** Error rate percentage
**Right Y-axis:** P95 latency

This helps correlate errors with latency spikes.

### 5. Query Stage Breakdown

**Widget Type:** Stacked Bar Chart

**Metrics:**
- `rpc.endpoint_timing.prepare_query`
- `rpc.endpoint_timing.execute`
- `rpc.endpoint_timing.cache_get`
- `rpc.endpoint_timing.rpc_start`

Shows where time is spent in request processing.

### 6. Storage Routing Performance Comparison

**Widget Type:** Grouped Bar Chart

**Metric:** `p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`

**Group by:** `storage_routing_mode`

Compare performance across different storage routing modes.

### 7. Referrer Performance Analysis

**Widget Type:** Top List

**Metric:** `p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`

**Group by:** `referrer`

**Sort:** Descending

Identify which callers experience the slowest responses.

## Key Performance Indicators (KPIs)

### Recommended SLO Targets

1. **P95 Latency:** < 1000ms
   - 95% of requests should complete within 1 second

2. **P99 Latency:** < 3000ms
   - 99% of requests should complete within 3 seconds

3. **Error Rate:** < 0.5%
   - Less than 0.5% of requests should fail

4. **Timeout Rate:** < 0.1%
   - Less than 0.1% of requests should timeout

5. **Fast Response Rate:** > 70% under 500ms
   - At least 70% of requests should complete within 500ms

### SLO Alerting

**P95 Latency Alert:**
```
avg(last_15m):p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} > 1000
```

**Error Rate Alert:**
```
avg(last_15m):(sum:rpc.request_error{endpoint_name:EndpointGetTrace}.as_rate() /
               sum:rpc.request_success{endpoint_name:EndpointGetTrace}.as_rate()) * 100 > 0.5
```

**Timeout Rate Alert:**
```
avg(last_15m):(sum:rpc.timeout_query{endpoint_name:EndpointGetTrace}.as_rate() /
               sum:rpc.request_success{endpoint_name:EndpointGetTrace}.as_rate()) * 100 > 0.1
```

## Usage in Code

### Automatic (Already Enabled)

All RPC endpoints automatically send distribution metrics. No code changes needed!

### Manual Timer Usage

If you need to add custom timing within the endpoint:

```python
from snuba.utils.metrics.timer import Timer
from snuba import environment

# Create timer
timer = Timer("custom_operation")

# ... do work ...
timer.mark("step1")

# ... more work ...
timer.mark("step2")

# Send as distribution metrics
timer.send_metrics_to(
    environment.metrics,
    tags={"operation": "custom"},
    use_distribution=True  # Enable histogram bucketing
)
```

### Backward Compatibility

To use the old timing behavior (not recommended):

```python
timer.send_metrics_to(
    environment.metrics,
    use_distribution=False  # Use old timing aggregation
)
```

## DataDog Dashboard Example

Here's a complete dashboard configuration for monitoring EndpointGetTrace:

```json
{
  "title": "EndpointGetTrace Performance",
  "widgets": [
    {
      "definition": {
        "title": "Latency Percentiles",
        "type": "timeseries",
        "requests": [
          {"q": "p50:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}"},
          {"q": "p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}"},
          {"q": "p99:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}"}
        ]
      }
    },
    {
      "definition": {
        "title": "Request Volume by Latency Bucket",
        "type": "distribution",
        "requests": [{
          "q": "rpc.endpoint_timing{endpoint_name:EndpointGetTrace}",
          "style": {"palette": "dog_classic"}
        }]
      }
    },
    {
      "definition": {
        "title": "Error Rate",
        "type": "query_value",
        "requests": [{
          "q": "(sum:rpc.request_error{endpoint_name:EndpointGetTrace}.as_count() / sum:rpc.request_success{endpoint_name:EndpointGetTrace}.as_count()) * 100",
          "conditional_formats": [
            {"comparator": ">", "value": 0.5, "palette": "white_on_red"},
            {"comparator": "<=", "value": 0.5, "palette": "white_on_green"}
          ]
        }]
      }
    }
  ]
}
```

## Troubleshooting

### High P95 Latency

1. Check the query stage breakdown to identify bottlenecks
2. Examine `rpc.endpoint_timing.execute` - if high, query optimization needed
3. Check `storage_routing_mode` - `HIGHEST_ACCURACY` mode is slower
4. Analyze `time_period` tag - longer time ranges scan more data

### High Error Rate

1. Check `rpc.timeout_query` for timeout issues
2. Check `rpc.OOM_query` for memory issues
3. Review `rpc.request_invalid` for validation errors
4. Examine Sentry for stack traces

### Uneven Distribution Across Referrers

1. Compare P95 by referrer to identify problematic clients
2. Check if certain referrers use `HIGHEST_ACCURACY` mode more
3. Analyze query patterns from different referrers

## Additional Resources

- **Snuba Timer Implementation:** `snuba/utils/metrics/timer.py`
- **RPC Endpoint Base Class:** `snuba/web/rpc/__init__.py`
- **EndpointGetTrace Implementation:** `snuba/web/rpc/v1/endpoint_get_trace.py`
- **Metrics Backend:** `snuba/utils/metrics/backends/`
- **Test Examples:** `tests/utils/metrics/test_timer.py`
