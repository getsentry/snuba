# Histogram Bucketing Example for EndpointGetTrace

## Visual Guide: Distribution Metrics vs Traditional Timing Metrics

### Before: Traditional Timing Metrics

With traditional `timing` metrics, you only get aggregated statistics:

```
EndpointGetTrace Timing (Traditional)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Metric: endpoint_timing

Available Data:
  • Average: 450ms
  • Min: 50ms
  • Max: 5000ms
  • P50: 300ms
  • P95: 1200ms
  • P99: 2500ms

❌ Problem: You can't answer questions like:
  • "How many requests took less than 100ms?"
  • "What percentage of requests are under 500ms?"
  • "How is latency distributed across different ranges?"
```

### After: Distribution Metrics with Histogram Bucketing

With `distribution` metrics, you can create custom buckets and count datapoints:

```
EndpointGetTrace Timing Distribution
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Metric: endpoint_timing (as distribution)

Histogram Buckets (1 hour window):
  < 100ms     ████████████████████████████████ 3,200 requests (32%)
  100-250ms   ██████████████████████████████████████████ 4,000 requests (40%)
  250-500ms   ████████████████ 1,600 requests (16%)
  500-1000ms  ████████ 800 requests (8%)
  1s-3s       ███ 300 requests (3%)
  > 3s        █ 100 requests (1%)

✅ Now you can answer:
  • "72% of requests complete under 250ms" ✓
  • "Only 1% take longer than 3 seconds" ✓
  • "We need to optimize the 8% in the 500-1000ms range" ✓
```

## DataDog Query Examples

### 1. Count Requests by Latency Bucket

```javascript
// Very Fast: < 100ms
sum:rpc.endpoint_timing{
  endpoint_name:EndpointGetTrace,
  rpc.endpoint_timing < 100
}.as_count()

// Fast: 100-500ms
sum:rpc.endpoint_timing{
  endpoint_name:EndpointGetTrace,
  rpc.endpoint_timing >= 100 AND rpc.endpoint_timing < 500
}.as_count()

// Acceptable: 500ms-1s
sum:rpc.endpoint_timing{
  endpoint_name:EndpointGetTrace,
  rpc.endpoint_timing >= 500 AND rpc.endpoint_timing < 1000
}.as_count()

// Slow: 1s-3s
sum:rpc.endpoint_timing{
  endpoint_name:EndpointGetTrace,
  rpc.endpoint_timing >= 1000 AND rpc.endpoint_timing < 3000
}.as_count()

// Very Slow: > 3s
sum:rpc.endpoint_timing{
  endpoint_name:EndpointGetTrace,
  rpc.endpoint_timing >= 3000
}.as_count()
```

### 2. Calculate Percentage in Each Bucket

```javascript
// Percentage of fast requests (< 500ms)
(
  sum:rpc.endpoint_timing{
    endpoint_name:EndpointGetTrace,
    rpc.endpoint_timing < 500
  }.as_count()
  /
  sum:rpc.endpoint_timing{
    endpoint_name:EndpointGetTrace
  }.as_count()
) * 100
```

### 3. Create Stacked Area Chart

In DataDog Dashboard, create a Stacked Area Chart with these queries:

```javascript
{
  "viz": "area",
  "requests": [
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing < 100}.as_count()",
      "display_type": "area",
      "style": {"palette": "cool"}
    },
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing >= 100 AND rpc.endpoint_timing < 500}.as_count()",
      "display_type": "area",
      "style": {"palette": "warm"}
    },
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing >= 500}.as_count()",
      "display_type": "area",
      "style": {"palette": "orange"}
    }
  ]
}
```

## Real-World Use Case Example

### Scenario: Optimizing GetTrace Performance

**Goal:** Reduce P95 latency from 1200ms to under 1000ms

#### Step 1: Analyze Current Distribution

```
Current State (Distribution View):
┌─────────────────────────────────────────┐
│ EndpointGetTrace Latency Distribution   │
├─────────────────────────────────────────┤
│ < 100ms:    ████████ 20%                │
│ 100-250ms:  ████████████ 30%            │
│ 250-500ms:  ████████ 20%                │
│ 500-1000ms: ████████ 20%                │
│ 1s-2s:      ███ 8%                      │
│ > 2s:       █ 2%                        │
│                                          │
│ P95: 1200ms                              │
│ Issue: 30% of requests > 500ms           │
└─────────────────────────────────────────┘
```

**Analysis:** The distribution shows:
- 70% of requests are reasonably fast (< 500ms)
- 30% of requests are slow (> 500ms)
- There's a clear bimodal distribution

**Action:** Investigate what causes the 30% slow requests.

#### Step 2: Segment by Referrer

```
Distribution by Referrer:
┌──────────────────────────────────┐
│ trace_view referrer:             │
│ < 100ms:    ████████████ 60%     │
│ 100-500ms:  ████████ 38%         │
│ > 500ms:    █ 2%                 │
│ P95: 250ms ✓                     │
└──────────────────────────────────┘

┌──────────────────────────────────┐
│ trace_waterfall referrer:        │
│ < 100ms:    █ 5%                 │
│ 100-500ms:  ████ 15%             │
│ > 500ms:    ████████████████ 80% │
│ P95: 2500ms ❌                   │
└──────────────────────────────────┘
```

**Discovery:** `trace_waterfall` is the problem referrer!

#### Step 3: Deep Dive into Slow Referrer

```
trace_waterfall by Storage Routing Mode:
┌──────────────────────────────────┐
│ NORMAL mode:                     │
│ < 500ms:    ████████████ 60%     │
│ > 500ms:    ████████ 40%         │
│ P95: 800ms                       │
└──────────────────────────────────┘

┌──────────────────────────────────┐
│ HIGHEST_ACCURACY mode:           │
│ < 500ms:    █ 5%                 │
│ > 500ms:    ███████████████ 95%  │
│ P95: 3200ms ❌                   │
└──────────────────────────────────┘
```

**Root Cause:** `trace_waterfall` uses `HIGHEST_ACCURACY` mode too often!

#### Step 4: Measure Impact of Fix

After optimizing `trace_waterfall`:

```
After Fix (Distribution View):
┌─────────────────────────────────────────┐
│ EndpointGetTrace Latency Distribution   │
├─────────────────────────────────────────┤
│ < 100ms:    ████████████ 30%            │
│ 100-250ms:  ████████████ 30%            │
│ 250-500ms:  ████████████ 30%            │
│ 500-1000ms: ████ 8%                     │
│ 1s-2s:      █ 1.5%                      │
│ > 2s:       █ 0.5%                      │
│                                          │
│ P95: 650ms ✓ (target achieved!)         │
│ Fast requests (< 500ms): 90% ✓          │
└─────────────────────────────────────────┘
```

**Success Metrics:**
- ✅ P95 reduced from 1200ms → 650ms (46% improvement)
- ✅ Requests < 500ms increased from 70% → 90%
- ✅ Slow requests (> 2s) reduced from 2% → 0.5%

## Bucket Configuration Examples

### Conservative Buckets (Strict SLOs)

```python
BUCKETS = {
    "excellent": "< 100ms",     # 🟢 Instant
    "good": "100-250ms",        # 🟢 Fast
    "acceptable": "250-500ms",  # 🟡 OK
    "slow": "500-1000ms",       # 🟠 Needs attention
    "very_slow": "1s-3s",       # 🔴 Problem
    "timeout_risk": "> 3s"      # 🔴 Critical
}

SLO Targets:
  • 80% should be "excellent" or "good" (< 250ms)
  • 95% should be under 1 second
  • 99% should be under 3 seconds
```

### Aggressive Buckets (Tight Performance Requirements)

```python
BUCKETS = {
    "instant": "< 50ms",        # 🟢 Cache hit
    "very_fast": "50-100ms",    # 🟢 Hot path
    "fast": "100-200ms",        # 🟢 Normal
    "acceptable": "200-500ms",  # 🟡 Warm path
    "slow": "500-1000ms",       # 🟠 Cold path
    "unacceptable": "> 1s"      # 🔴 Problem
}

SLO Targets:
  • 50% should be instant (< 50ms)
  • 90% should be fast (< 200ms)
  • 99% should be acceptable (< 500ms)
```

### Realistic Buckets (Balanced Approach)

```python
BUCKETS = {
    "fast": "< 500ms",          # 🟢 Good user experience
    "acceptable": "500-1000ms", # 🟡 Tolerable
    "slow": "1s-3s",            # 🟠 Poor experience
    "timeout": "> 3s"           # 🔴 Unacceptable
}

SLO Targets:
  • 70% should be fast (< 500ms)
  • 95% should be under 1 second
  • 99.5% should be under 3 seconds
```

## Dashboard Widget Examples

### Widget 1: Histogram Visualization

```json
{
  "title": "EndpointGetTrace Latency Distribution",
  "type": "distribution",
  "requests": [{
    "q": "rpc.endpoint_timing{endpoint_name:EndpointGetTrace}",
    "style": {
      "palette": "dog_classic"
    }
  }],
  "xaxis": {
    "label": "Latency (ms)",
    "scale": "log"
  },
  "yaxis": {
    "label": "Request Count"
  }
}
```

### Widget 2: Bucket Trend Over Time

```json
{
  "title": "Requests by Latency Bucket (7d trend)",
  "type": "timeseries",
  "requests": [
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing < 500}.as_count()",
      "display_type": "area",
      "style": {"palette": "green"}
    },
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing >= 500 AND rpc.endpoint_timing < 1000}.as_count()",
      "display_type": "area",
      "style": {"palette": "orange"}
    },
    {
      "q": "sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing >= 1000}.as_count()",
      "display_type": "area",
      "style": {"palette": "red"}
    }
  ]
}
```

### Widget 3: SLO Compliance Gauge

```json
{
  "title": "SLO Compliance: % Requests < 500ms",
  "type": "query_value",
  "requests": [{
    "q": "(sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace, rpc.endpoint_timing < 500}.as_count() / sum:rpc.endpoint_timing{endpoint_name:EndpointGetTrace}.as_count()) * 100",
    "conditional_formats": [
      {"comparator": ">=", "value": 70, "palette": "white_on_green"},
      {"comparator": ">=", "value": 50, "palette": "white_on_yellow"},
      {"comparator": "<", "value": 50, "palette": "white_on_red"}
    ]
  }],
  "precision": 1,
  "unit": "%"
}
```

## Key Takeaways

✅ **Distribution metrics enable:**
1. Custom bucket analysis
2. Percentage calculations (e.g., "% under 500ms")
3. Trend analysis by latency ranges
4. Identification of bimodal distributions
5. Precise SLO tracking

✅ **Use cases:**
- Performance optimization targeting
- SLO compliance monitoring
- Identifying problematic segments
- Before/after comparisons
- Capacity planning

✅ **Best practices:**
- Choose bucket boundaries based on your SLOs
- Monitor trends over time, not just current state
- Segment by meaningful dimensions (referrer, mode, etc.)
- Set alerts on both percentiles AND bucket percentages
- Review distribution regularly to catch drift

## Further Reading

- See `ENDPOINT_TIMING_METRICS.md` for complete metric reference
- See `CHANGES_SUMMARY.md` for implementation details
- DataDog Distribution Metrics: https://docs.datadoghq.com/metrics/distributions/
