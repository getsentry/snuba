# Quick Reference: EndpointGetTrace Distribution Metrics

## TL;DR

**What changed:** All RPC endpoint timing metrics now use **distribution metrics** instead of timing metrics, enabling histogram bucketing in DataDog.

**Impact:** You can now count how many requests fall into specific latency buckets (e.g., "How many requests took < 100ms?").

---

## Quick Queries

### Count Requests by Latency

```javascript
// Fast (< 500ms)
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing < 500}.as_count()

// Slow (> 1000ms)
sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing > 1000}.as_count()
```

### Calculate Percentages

```javascript
// % of requests under 500ms
(sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing < 500}.as_count() /
 sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()) * 100
```

### View Distribution

```javascript
// Histogram widget
distribution:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}
```

---

## Key Metrics

| Metric | Description | Use For |
|--------|-------------|---------|
| `rpc.endpoint_timing` | Total request duration | Overall performance |
| `rpc.endpoint_timing.execute` | ClickHouse query time | Query optimization |
| `rpc.endpoint_timing.prepare_query` | Query building time | Code optimization |
| `rpc.endpoint_timing.cache_get` | Cache read time | Cache performance |

---

## Important Tags

| Tag | Values | Use For |
|-----|--------|---------|
| `endpoint_name` | `EndpointGetTrace` | Filter to this endpoint |
| `referrer` | `trace_view`, `trace_waterfall`, etc. | Identify client |
| `storage_routing_mode` | `NORMAL`, `HIGHEST_ACCURACY` | Compare modes |
| `time_period` | `lte_1_hour`, `lte_1_day`, etc. | Query window analysis |

---

## Recommended SLOs

| SLO | Target | Query |
|-----|--------|-------|
| P95 Latency | < 1000ms | `p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}` |
| P99 Latency | < 3000ms | `p99:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}` |
| Fast Response Rate | > 70% under 500ms | See percentage query above |
| Error Rate | < 0.5% | `(sum:rpc.request_error / sum:rpc.request_success) * 100` |

---

## Latency Buckets

Suggested bucket definitions for analysis:

```
⚡ < 100ms     - Excellent (cache hits, hot path)
🟢 100-250ms   - Good (normal operation)
🟢 250-500ms   - Acceptable (warm path)
🟡 500-1000ms  - Slow (needs investigation)
🟠 1s-3s       - Problem (optimize urgently)
🔴 > 3s        - Critical (timeout risk)
```

---

## 5-Minute Dashboard Setup

1. **Create Distribution Widget**
   - Type: Distribution
   - Metric: `rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
   - Shows: Histogram of all requests

2. **Create Percentile Widget**
   - Type: Timeseries
   - Metrics: `p50`, `p95`, `p99` of `rpc.endpoint_timing`
   - Shows: Latency trends over time

3. **Create Bucket Widget**
   - Type: Query Value
   - Metric: Percentage under 500ms (see query above)
   - Shows: SLO compliance

---

## Common Use Cases

### Find Slow Referrers
```javascript
p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} by {referrer}
```

### Compare Storage Modes
```javascript
p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} by {storage_routing_mode}
```

### Identify Query Bottlenecks
```javascript
avg:rpc.endpoint_timing.execute{endpoint_name:EndpointGetTrace}
avg:rpc.endpoint_timing.prepare_query{endpoint_name:EndpointGetTrace}
```

### Track Improvement
```javascript
// Before optimization
avg(p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}, last_7d)

// After optimization
avg(p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}, last_1d)
```

---

## Troubleshooting

| Problem | Check This | Fix |
|---------|-----------|-----|
| High P95 | `rpc.endpoint_timing.execute` | Optimize query |
| High P95 | `storage_routing_mode` tag | Review mode usage |
| Timeouts | `rpc.timeout_query` counter | Increase timeout or optimize |
| Memory errors | `rpc.OOM_query` counter | Reduce data scanned |

---

## Documentation

📖 **Full Guides:**
- `ENDPOINT_TIMING_METRICS.md` - Complete reference
- `HISTOGRAM_BUCKETING_EXAMPLE.md` - Practical examples
- `CHANGES_SUMMARY.md` - Technical details

---

## Example Alert

```yaml
# Alert when P95 exceeds SLO
name: "EndpointGetTrace P95 Latency High"
query: "avg(last_15m):p95:rpc.endpoint_timing{endpoint_name:EndpointGetTrace} > 1000"
message: "P95 latency for EndpointGetTrace is above 1000ms. Check dashboard for details."
```

---

## Need Help?

1. Check if metric exists: `rpc.endpoint_timing{endpoint_name:EndpointGetTrace}`
2. Verify tags are populated: Add `by {referrer}` to any query
3. Review stage breakdown: Check `rpc.endpoint_timing.*` metrics
4. Examine errors: Check `rpc.request_error`, `rpc.timeout_query`

---

**Generated for:** `EndpointGetTrace` timing analysis
**Last Updated:** Distribution metrics implementation
**Status:** ✅ Production Ready
