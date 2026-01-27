# Snuba Project - All Issues Analysis (Last 24 Hours)
**Date:** January 27, 2026
**Organization:** sentry
**Total Issues Analyzed:** 33

---

## Issue Summaries

### SNUBA-1VD - Socket Shutdown Error (CRITICAL - 35M+ events)
**Status:** Unresolved | **Assigned:** dmitrii.fedorov@sentry.io | **Actionability:** Super Low
**Occurrences:** 35,776,831 events | **First Seen:** May 2020 | **Last Seen:** Jan 27, 2026

Error on socket shutdown during ClickHouse health checks with "Transport endpoint is not connected" (errno 107). This is a long-standing infrastructure issue affecting health check endpoints. Despite super low actionability rating, the massive event volume (35M+) makes this critical for system noise and potential monitoring impact. Seer suggests adding socket error handling to ClickhousePool.execute cleanup to prevent broken connections from being returned to the pool.

---

### SNUBA-1V1 - ClickHouse Connection EOF Error (CRITICAL - 55M+ events)
**Status:** Unresolved | **Assigned:** Markus Unterwaditzer | **Actionability:** Super Low
**Occurrences:** 55,332,545 events | **First Seen:** May 2020 | **Last Seen:** Jan 27, 2026

"Unexpected EOF while reading bytes" errors during ClickHouse ping operations. This is the highest volume issue with over 55 million occurrences since 2020. While marked as super low actionability, the sheer volume indicates a fundamental connection management issue that should be addressed to reduce system noise and improve observability.

---

### SNUBA-32F - DateTime Validation Error (2M+ events)
**Status:** Unresolved | **Actionability:** Medium
**Occurrences:** 2,052,526 events | **First Seen:** Feb 2023 | **Last Seen:** Jan 27, 2026

Timestamp validation errors where datetime strings with timezone info are rejected. Error message: "timestamp AS `_snuba_timestamp` requires datetime conditions: '2026-01-26T14:11:12.558000+00:00' is not a valid datetime". This affects the events dataset deletions endpoint. Medium actionability suggests this is a known pattern that could be fixed with proper datetime parsing improvements.

---

### SNUBA-9VC - Missing project_id Validation (HIGH PRIORITY)
**Status:** Unresolved | **Actionability:** HIGH
**Occurrences:** 32,779 events | **First Seen:** Oct 2025 | **Last Seen:** Jan 27, 2026

Validation error for group_attributes entity: "missing required conditions for project_id". Affects dashboard table widgets in the US environment. High actionability indicates this is a well-understood issue with a clear fix path. This should be prioritized as it impacts customer-facing dashboards and has high actionability.

---

### SNUBA-9VD - Missing project_id Validation (HIGH PRIORITY)
**Status:** Unresolved | **Actionability:** HIGH
**Occurrences:** 29,667 events | **First Seen:** Oct 2025 | **Last Seen:** Jan 27, 2026

Duplicate of SNUBA-9VC - same validation error for group_attributes entity missing project_id conditions. Affects dashboard table widgets. High actionability rating makes this a priority fix. These two issues (9VC and 9VD) may be the same root cause and should be investigated together.

---

### SNUBA-32P - DateTime Validation in Subscriptions (257K events)
**Status:** IGNORED (archived forever) | **Assigned:** evan.hicks@sentry.io | **Actionability:** Low
**Occurrences:** 257,830 events | **First Seen:** Feb 2023 | **Last Seen:** Jan 27, 2026

Subscription executor receiving invalid datetime formats (Unix timestamps as strings like '1726374214000'). Issue has been intentionally ignored/archived. While it has significant volume, the team has decided not to fix this, possibly due to backward compatibility concerns or upstream data issues.

---

### SNUBA-A3N - DataDog StatsD Connection Error (22K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 22,935 events | **First Seen:** Jan 2026 | **Last Seen:** Jan 27, 2026

Connection refused errors when submitting packets to DataDog StatsD. Relatively recent issue (started Jan 15, 2026) affecting metrics reporting. Super low actionability suggests this may be an infrastructure/networking issue rather than a code bug. May need infrastructure team investigation.

---

### SNUBA-5C4 - ClickHouse Optimize Timeout (500 events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 500 events | **First Seen:** Sep 2024 | **Last Seen:** Jan 27, 2026

Timeout errors during optimize operations on the errors storage. Occurs during partition optimization in the background optimize cron job. Low frequency suggests this is an occasional issue during large optimization tasks. May be acceptable operational noise or indicate need for timeout adjustments.

---

### SNUBA-9XX - Distributed DDL Task Timeout (1.5K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 1,543 events | **First Seen:** Nov 2025 | **Last Seen:** Jan 27, 2026

Distributed DDL tasks timing out after 180 seconds during delete operations on search_issues storage cluster. ClickHouse reports tasks will execute in background. This affects the lightweight deletions consumer. May indicate cluster coordination issues or need for increased DDL timeout settings.

---

### SNUBA-3D6 - ClickHouse Distributed Table Alias Error (3.7K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 3,721 events | **First Seen:** Jul 2023 | **Last Seen:** Jan 27, 2026

"Distributed table should have an alias when distributed_product_mode set to local" errors in trace queries. Affects the Snuba admin clickhouse_trace_query endpoint. This is a ClickHouse SQL query generation issue where distributed tables in subqueries need aliases. Query needs rewriting to add proper aliases.

---

### SNUBA-8P5 - HTTPLib2 Timeout Warning (77K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 77,709 events | **First Seen:** May 2025 | **Last Seen:** Jan 27, 2026

Warning about httplib2 transport not supporting per-request timeout settings. Affects Google API integration in Snuba admin. This is a library limitation warning, not an actual error. Low impact but creates noise in error reporting.

---

### SNUBA-9VQ - Google API Permission Denied (811 events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 811 events | **First Seen:** Nov 2025 | **Last Seen:** Jan 27, 2026

Google Cloud Identity API returning 403 permission denied errors when looking up group information. Affects Snuba admin trace queries. This appears to be a configuration/permissions issue with the Google service account, not a code bug.

---

### SNUBA-9VP - Google API Permission Denied Warning (812 events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 812 events | **First Seen:** Nov 2025 | **Last Seen:** Jan 27, 2026

Related to SNUBA-9VQ - same Google API permission denied errors but logged at warning level. These two issues are likely the same root cause (Google service account permissions). Should be fixed together by updating IAM permissions.

---

### SNUBA-474 - Kafka Purge Queue Error (CRITICAL - 2.8M+ events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 2,815,538 events | **First Seen:** Jan 2024 | **Last Seen:** Jan 27, 2026

Message production failures in Rust consumer with "PurgeQueue (Local: Purged in queue)" errors. Affects generic_metrics_distributions consumer. This is Kafka client behavior when messages are dropped from the queue, possibly due to throughput issues or consumer lag. Very high volume indicates a systemic issue.

---

### SNUBA-475 - Kafka Purge Inflight Error (280K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 280,781 events | **First Seen:** Jan 2024 | **Last Seen:** Jan 27, 2026

Similar to SNUBA-474 but for in-flight messages: "PurgeInflight (Local: Purged in flight)". Affects the same generic_metrics_distributions consumer. These purge errors suggest backpressure or consumer throughput issues that need performance investigation.

---

### SNUBA-9C6 - SPANS Storage Not Defined (280K events)
**Status:** Unresolved | **Actionability:** Medium
**Occurrences:** 280,498 events | **First Seen:** Jul 2025 | **Last Seen:** Jan 27, 2026

"StorageSetKey.SPANS is not defined in the CLUSTERS setting for this environment" warning in Snuba admin. This is a configuration issue where the SPANS storage set is referenced but not configured. Medium actionability suggests straightforward fix by adding proper cluster configuration.

---

### SNUBA-9E1 - UnboundLocalError in Subscriptions (MEDIUM PRIORITY)
**Status:** Unresolved | **Actionability:** MEDIUM
**Occurrences:** 1,434 events | **First Seen:** Aug 2025 | **Last Seen:** Jan 27, 2026

UnboundLocalError accessing 'transformed_message' variable in subscription executor. Occurs when allocation policy violations happen and the exception handling tries to reference an uninitialized variable. Medium actionability indicates this is a clear bug in error handling code that should be fixed.

---

### SNUBA-A4D - Double-Distributed Query Error (27 events - NEW)
**Status:** Unresolved (New) | **Actionability:** Low
**Occurrences:** 27 events | **First Seen:** Jan 27, 2026 | **Last Seen:** Jan 27, 2026

"Double-distributed IN/JOIN subqueries is denied" ClickHouse error in EAP timeseries queries. Very new issue (started today). The query needs rewriting to use local tables in subqueries or GLOBAL keyword. Affects EndpointTimeSeries v1 RPC endpoint.

---

### SNUBA-9JH - StopIteration Error (MEDIUM PRIORITY)
**Status:** Unresolved | **Actionability:** MEDIUM
**Occurrences:** 9,727 events | **First Seen:** Sep 2025 | **Last Seen:** Jan 27, 2026

StopIteration exception in rate limiting code when iterating over Redis pipeline results. The code expects specific pipeline commands but iterator runs out. Medium actionability indicates this is a logical bug in the rate limiter that should be fixed to prevent crashes.

---

### SNUBA-4VF - Task Timeout Warning (CRITICAL - 5.8M+ events)
**Status:** Unresolved | **Assigned:** snuba team | **Actionability:** Super Low
**Occurrences:** 5,837,106 events | **First Seen:** May 2024 | **Last Seen:** Jan 27, 2026

"Timeout reached while waiting for tasks to finish" in Rust consumer strategy. Affects generic_metrics_distributions consumer with nearly 6 million occurrences. This indicates systematic performance issues with ClickHouse write tasks timing out. Despite low actionability, the volume makes this critical for investigation.

---

### SNUBA-A4C - Double-Distributed Query Error (6 events - NEW)
**Status:** Unresolved (New) | **Actionability:** Low
**Occurrences:** 6 events | **First Seen:** Jan 27, 2026 | **Last Seen:** Jan 27, 2026

Same as SNUBA-A4D but affecting EndpointTraceItemTable. Both issues started today and are the same ClickHouse double-distributed query problem. Should be fixed together by rewriting queries to use GLOBAL keyword or local tables.

---

### SNUBA-9WM - Kafka Commit Failed (557 events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 557 events | **First Seen:** Nov 2025 | **Last Seen:** Jan 27, 2026

Kafka commit failures with "UNKNOWN_MEMBER_ID" error in subscriptions executor. Consumer group coordination issue where broker doesn't recognize the consumer member. Could indicate network issues, rebalancing problems, or session timeout issues.

---

### SNUBA-9Y7 - Subscription Executor Timeout (261 events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 261 events | **First Seen:** Nov 2025 | **Last Seen:** Jan 27, 2026

TimeoutError in subscription executor when waiting for query results during shutdown. Queries don't complete within the join timeout period. Low frequency suggests this is acceptable behavior during graceful shutdown, though timeout tuning could reduce occurrences.

---

### SNUBA-A3X - Redis Cluster Connection Error (2 events - NEW)
**Status:** Unresolved (New) | **Actionability:** Super Low
**Occurrences:** 2 events | **First Seen:** Jan 20, 2026 | **Last Seen:** Jan 27, 2026

Redis cluster connection timeout in allocation policy quota checking. Very low frequency but indicates potential Redis connectivity issues. Could be transient network problems or Redis cluster health issues.

---

### SNUBA-9JX - Redis Timeout in Query Flags (96K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 96,377 events | **First Seen:** Sep 2025 | **Last Seen:** Jan 27, 2026

Timeout errors when connecting to Redis to load project query flags for the post-replacement consistency enforcer. High volume indicates systematic Redis connectivity or performance issues. May need Redis cluster investigation or connection pool tuning.

---

### SNUBA-9TN - Column Type Error (RESOLVED - HIGH PRIORITY)
**Status:** RESOLVED | **Assigned:** txiao@sentry.io | **Actionability:** HIGH
**Occurrences:** 263 events | **First Seen:** Oct 2025 | **Last Seen:** Jan 27, 2026

ValueError "Unknown column type: None" in trace item table request visitor. Issue is RESOLVED showing successful fix for formula handling bug where 0 was treated as None. Seer analysis suggests using "is not None" checks instead of truthy checks. Good example of high actionability issue being fixed.

---

### SNUBA-A3A - Profile Events Error (166 events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 166 events | **First Seen:** Jan 2026 | **Last Seen:** Jan 27, 2026

TypeError "missing 1 required positional argument: 'user'" when gathering profile events in admin trace queries. Code change broke function signature compatibility. Low frequency but clear bug that should be quick fix.

---

### SNUBA-A4B - IndexError Pop from Empty List (MEDIUM PRIORITY - NEW)
**Status:** Unresolved (New) | **Actionability:** MEDIUM
**Occurrences:** 1 event | **First Seen:** Jan 26, 2026 | **Last Seen:** Jan 26, 2026

IndexError when popping from an empty list in clickhouse_trace_query endpoint. Only 1 occurrence but medium actionability suggests this is a clear bug. Stack trace is filtered but error message is clear.

---

### SNUBA-9JR - Redis Timeout in Rate Limiting (25K events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 24,996 events | **First Seen:** Sep 2025 | **Last Seen:** Jan 27, 2026

Redis connection timeouts during rate limit checks for concurrent queries. Affects multiple query endpoints. 25K occurrences indicate systematic Redis performance or connectivity issues impacting rate limiting functionality.

---

### SNUBA-7ZF - Redis Pipeline Timeout (CRITICAL - 608K events)
**Status:** Unresolved | **Assigned:** spans team | **Actionability:** Super Low
**Occurrences:** 608,519 events | **First Seen:** Mar 2025 | **Last Seen:** Jan 27, 2026

Redis pipeline timeouts when finishing rate limit requests (ZINCRBY commands). Over 600K occurrences affecting subscription executors. Critical volume indicating serious Redis performance issues or network problems. Assigned to spans team for investigation.

---

### SNUBA-A2F - Memory Limit Exceeded (48 events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 48 events | **First Seen:** Dec 2025 | **Last Seen:** Jan 27, 2026

ClickHouse query exceeded memory limit of 18.63 GiB when reading EAP item attributes. Query tried to allocate additional chunks after hitting the limit. Affects trace item attribute names endpoint. Query optimization or memory limit increase needed.

---

### SNUBA-A3V - Allocation Policy Rejection (17 events)
**Status:** Unresolved | **Actionability:** Super Low
**Occurrences:** 17 events | **First Seen:** Jan 2026 | **Last Seen:** Jan 27, 2026

Info-level logs when queries are rejected due to allocation policies. This is expected behavior when rate limits or resource limits are hit. Low frequency and info level suggest this is working as designed.

---

### SNUBA-4GY - Duplicate Group Replacement (1.2K events)
**Status:** Unresolved | **Actionability:** Low
**Occurrences:** 1,192 events | **First Seen:** Mar 2024 | **Last Seen:** Jan 27, 2026

"Skipping duplicate group replacement" errors in the errors replacer. Indicates the system is receiving duplicate replacement messages for the same group. This could be expected deduplication behavior or indicate upstream duplicate message production.

---

## Most Urgent Issues - Priority Ranking

### CRITICAL (Immediate Action Needed)

1. **SNUBA-1V1** (55M+ events) - ClickHouse EOF errors
   - Highest event volume, long-standing issue
   - Infrastructure-level connection management problem
   - Assigned but not resolved since 2020

2. **SNUBA-1VD** (35M+ events) - Socket shutdown errors
   - Second highest volume
   - Has Seer analysis with suggested fix
   - Long-standing health check issue

3. **SNUBA-4VF** (5.8M+ events) - Task timeouts
   - Nearly 6 million events
   - Assigned to snuba team
   - Indicates systematic performance problems

4. **SNUBA-474** (2.8M+ events) - Kafka purge queue
   - Massive volume indicating throughput issues
   - Affects metrics ingestion pipeline

### HIGH PRIORITY (Action This Week)

5. **SNUBA-9VC** & **SNUBA-9VD** (62K combined) - Missing project_id validation
   - **HIGH ACTIONABILITY** - Clear fix path
   - Customer-facing dashboard impact
   - Recent issues (started Oct 2025)
   - Should be investigated together as likely same root cause

6. **SNUBA-7ZF** (608K events) - Redis timeout
   - Assigned to spans team
   - Systematic Redis performance issue
   - Affecting rate limiting infrastructure

7. **SNUBA-32F** (2M+ events) - DateTime validation
   - Medium actionability
   - Clear parsing issue with solution path
   - High volume justifies priority

### MEDIUM PRIORITY (Action This Month)

8. **SNUBA-9E1** (1.4K events) - UnboundLocalError
   - Medium actionability
   - Clear bug in error handling
   - Causes subscription executor crashes

9. **SNUBA-9JH** (9.7K events) - StopIteration error
   - Medium actionability
   - Logical bug in rate limiter
   - Should be straightforward fix

10. **SNUBA-A4B** (1 event) - IndexError
    - Medium actionability despite low volume
    - Recent bug that should be quick fix

### MONITORING REQUIRED

11. **SNUBA-A4D & SNUBA-A4C** (33 combined events) - Double-distributed queries
    - Very new (started today Jan 27)
    - Same root cause, different endpoints
    - Monitor for volume increase

12. **SNUBA-9JX** (96K events) - Redis timeouts in flags
    - Indicates Redis cluster issues
    - May be related to SNUBA-7ZF

## Key Themes & Recommendations

### Infrastructure Issues
- **Redis Performance**: Multiple issues (7ZF, 9JX, 9JR, A3X) indicate Redis cluster problems
- **ClickHouse Connections**: Issues 1V1 and 1VD show fundamental connection management issues
- **Kafka Throughput**: Issues 474 and 475 suggest consumer lag or backpressure

### Quick Wins (High Actionability)
- Fix SNUBA-9VC/9VD validation issues (HIGH actionability)
- Fix SNUBA-9E1 UnboundLocalError (MEDIUM actionability)
- Fix SNUBA-9JH StopIteration error (MEDIUM actionability)

### Performance Investigation Needed
- Redis cluster health and connection pooling
- ClickHouse connection management and pooling
- Kafka consumer throughput and configuration

### Configuration Issues
- SNUBA-9C6: Add SPANS cluster configuration
- SNUBA-9VQ/9VP: Fix Google API permissions
- SNUBA-32P: Already ignored, but 257K events is noise

---

**Analysis completed:** January 27, 2026
