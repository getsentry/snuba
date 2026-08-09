"""EAP Stats: categorize historical EAP traffic from the querylog and aggregate
resource usage (bytes, duration, and optionally ClickHouse ProfileEvents for
CPU / memory / IO / network).

Used by the snuba-admin "EAP Stats" page.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any

from google.protobuf.json_format import Parse
from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import TraceItemStatsRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest

from snuba.admin.clickhouse.common import get_ro_query_node_connection
from snuba.admin.clickhouse.querylog import run_querylog_query
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.query_info import extract_query_info

logger = logging.getLogger(__name__)

# Hard caps. Defaults stay interactive-sized; operators can raise max_rows up to
# _MAX_ROWS for whole-dataset analysis. ClickHouse queries for this tool always
# run with max_threads=0 (use all available cores).
_MAX_HOURS = 24 * 7
_DEFAULT_HOURS = 6
_MAX_ROWS = 500_000
_DEFAULT_ROWS = 1000
_MAX_EXAMPLES_PER_TYPE = 5
# ProfileEvents lookup still batches query ids; keep this large enough for
# full-dataset runs while avoiding giant IN lists.
_MAX_PROFILE_QUERY_IDS = 50_000
# EAP Stats intentionally uses all ClickHouse cores.
_EAP_STATS_MAX_THREADS = 0

# Prefer the normal run_query path (`dataset = eap`). Storage-routed requests
# also write a duplicate `storage_routing` row for the same request; counting
# both roughly doubles bytes/duration/query totals. Estimation queries against
# the outcomes table also land as `eap` and are filtered out below.
_EAP_DATASETS = ("eap",)

# Storage used to look up system.query_log ProfileEvents for EAP queries.
_EAP_PROFILE_STORAGE = "eap_items"

# Map of ClickHouse ProfileEvents keys -> our aggregate resource buckets.
_PROFILE_EVENT_MAP: dict[str, str] = {
    # CPU
    "UserTimeMicroseconds": "cpu_user_us",
    "SystemTimeMicroseconds": "cpu_system_us",
    "OSCPUVirtualTimeMicroseconds": "cpu_virtual_us",
    "RealTimeMicroseconds": "realtime_us",
    # Memory
    "MemoryTrackerUsage": "memory_usage_bytes",
    "MemoryTrackerPeakUsage": "memory_peak_bytes",
    # IO
    "SelectedBytes": "io_selected_bytes",
    "SelectedRows": "io_selected_rows",
    "ReadCompressedBytes": "io_read_compressed_bytes",
    "CompressedReadBufferBytes": "io_compressed_read_buffer_bytes",
    "ReadBufferFromFileDescriptorReadBytes": "io_fd_read_bytes",
    "WriteBufferFromFileDescriptorWriteBytes": "io_fd_write_bytes",
    # Network
    "NetworkReceiveBytes": "network_receive_bytes",
    "NetworkSendBytes": "network_send_bytes",
    "NetworkReceiveElapsedMicroseconds": "network_receive_us",
    "NetworkSendElapsedMicroseconds": "network_send_us",
}


@dataclass
class EapQueryAnalysisRequest:
    hours: int = _DEFAULT_HOURS
    max_rows: int = _DEFAULT_ROWS
    referrer: str | None = None
    organization_id: int | None = None
    referrer_contains: str | None = None
    # When true, look up ProfileEvents from system.query_log on the EAP cluster
    # for the sampled query ids (CPU / mem / IO / network).
    include_profile_events: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> EapQueryAnalysisRequest:
        if not data:
            return cls()
        hours = max(1, min(int(data.get("hours", _DEFAULT_HOURS)), _MAX_HOURS))
        max_rows = max(1, min(int(data.get("max_rows", _DEFAULT_ROWS)), _MAX_ROWS))

        organization_id = data.get("organization_id")
        if organization_id is not None and organization_id != "":
            organization_id = int(organization_id)
        else:
            organization_id = None

        include_profile_events = bool(data.get("include_profile_events", True))

        return cls(
            hours=hours,
            max_rows=max_rows,
            referrer=data.get("referrer") or None,
            organization_id=organization_id,
            referrer_contains=data.get("referrer_contains") or None,
            include_profile_events=include_profile_events,
        )


@dataclass
class ResourceTotals:
    """Aggregated resource counters.

    Profile-derived fields are 0 when enrichment is off or no ProfileEvents were
    found for the sampled query ids.
    """

    # Always available from snuba querylog.
    bytes_scanned: int = 0
    duration_ms: int = 0
    # From ClickHouse ProfileEvents (optional).
    cpu_user_us: int = 0
    cpu_system_us: int = 0
    cpu_virtual_us: int = 0
    realtime_us: int = 0
    memory_usage_bytes: int = 0
    memory_peak_bytes: int = 0
    io_selected_bytes: int = 0
    io_selected_rows: int = 0
    io_read_compressed_bytes: int = 0
    io_compressed_read_buffer_bytes: int = 0
    io_fd_read_bytes: int = 0
    io_fd_write_bytes: int = 0
    network_receive_bytes: int = 0
    network_send_bytes: int = 0
    network_receive_us: int = 0
    network_send_us: int = 0
    # How many source CH queries contributed ProfileEvents to these totals.
    profile_events_matched: int = 0

    def add_profile_events(self, events: dict[str, int]) -> None:
        matched = False
        for pe_key, field_name in _PROFILE_EVENT_MAP.items():
            value = int(events.get(pe_key) or 0)
            if value:
                matched = True
            setattr(self, field_name, getattr(self, field_name) + value)
        if matched:
            self.profile_events_matched += 1

    def add_other(self, other: ResourceTotals) -> None:
        for f in self.__dataclass_fields__:
            setattr(self, f, getattr(self, f) + getattr(other, f))

    @property
    def cpu_total_us(self) -> int:
        # Prefer OSCPUVirtualTimeMicroseconds when present: it already accounts
        # for threaded user/system work. Falling back to user+system avoids
        # double-counting when both families are populated.
        if self.cpu_virtual_us:
            return self.cpu_virtual_us
        return self.cpu_user_us + self.cpu_system_us

    @property
    def io_total_bytes(self) -> int:
        return self.io_selected_bytes + self.io_read_compressed_bytes

    @property
    def network_total_bytes(self) -> int:
        return self.network_receive_bytes + self.network_send_bytes


@dataclass
class QueryTypeBucket:
    query_type: str
    query_count: int = 0
    queries_profiled: int = 0
    resources: ResourceTotals = field(default_factory=ResourceTotals)
    avg_bytes_scanned: float = 0.0
    avg_duration_ms: float = 0.0
    pct_of_bytes: float = 0.0
    pct_of_queries: float = 0.0
    pct_of_cpu: float = 0.0
    pct_of_memory_peak: float = 0.0
    pct_of_io: float = 0.0
    pct_of_network: float = 0.0
    # Share of this bucket's own queries / bytes that had ProfileEvents.
    pct_queries_profiled: float = 0.0
    pct_bytes_profiled: float = 0.0
    by_trace_item_type: dict[str, int] = field(default_factory=dict)
    by_filter_profile: dict[str, int] = field(default_factory=dict)
    by_referrer: dict[str, int] = field(default_factory=dict)
    examples: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ProfileCoverage:
    """How much of the sample is backed by ClickHouse ProfileEvents.

    Use this to judge whether CPU / mem / IO / network aggregates are
    representative. Bytes-weighted coverage is usually the better signal:
    if 20% of queries but 80% of bytes are profiled, cost totals are still
    fairly trustworthy.
    """

    enabled: bool = False
    # Request-level (snuba querylog rows).
    queries_total: int = 0
    queries_with_query_id: int = 0
    queries_profiled: int = 0
    # ClickHouse query_id level.
    query_ids_sampled: int = 0
    query_ids_looked_up: int = 0
    query_ids_matched: int = 0
    query_ids_capped: bool = False
    # Cost mass covered by profiled requests.
    bytes_total: int = 0
    bytes_profiled: int = 0
    duration_ms_total: int = 0
    duration_ms_profiled: int = 0
    # Percentages (0-100).
    pct_queries_profiled: float = 0.0
    pct_queries_with_query_id_profiled: float = 0.0
    pct_query_ids_matched: float = 0.0
    pct_bytes_profiled: float = 0.0
    pct_duration_profiled: float = 0.0


@dataclass
class EapQueryAnalysisResult:
    hours: int
    max_rows: int
    rows_scanned: int
    rows_categorized: int
    rows_failed: int
    profile_events_enabled: bool
    profile_events_matched: int
    profile_coverage: ProfileCoverage
    total_resources: ResourceTotals
    by_query_type: list[QueryTypeBucket]
    by_filter_profile: list[dict[str, Any]]
    by_trace_item_type: list[dict[str, Any]]
    by_referrer: list[dict[str, Any]]


def _schema_table_name() -> str:
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    return schema.get_table_name()


def _escape_literal(value: str) -> str:
    """Escape a string for safe inclusion in a single-quoted SQL literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_fetch_sql(req: EapQueryAnalysisRequest) -> str:
    table = _schema_table_name()
    dataset_list = ", ".join(f"'{d}'" for d in _EAP_DATASETS)
    where = [
        f"timestamp > now() - INTERVAL {int(req.hours)} HOUR",
        f"dataset IN ({dataset_list})",
    ]
    if req.referrer:
        where.append(f"referrer = '{_escape_literal(req.referrer)}'")
    if req.referrer_contains:
        # Use positionCaseInsensitive for literal substring matching. ClickHouse
        # 25.x does not support LIKE ... ESCAPE, and LIKE would treat _/% as
        # wildcards for inputs like "eap_items".
        where.append(
            f"positionCaseInsensitive(referrer, '{_escape_literal(req.referrer_contains)}') > 0"
        )
    if req.organization_id is not None:
        where.append(f"organization = {int(req.organization_id)}")

    # Drop routing estimation queries. They share dataset=eap and the customer
    # referrer, but hit the outcomes tables purely to pick a sampling tier.
    where.append(
        "NOT arrayExists("
        "t -> positionCaseInsensitive(t, 'outcomes') > 0, "
        "clickhouse_queries.clickhouse_table)"
    )

    where_sql = " AND ".join(where)
    return f"""
        SELECT
            request_id,
            timestamp,
            referrer,
            dataset,
            organization,
            duration_ms,
            status,
            request_body,
            clickhouse_queries.query_id,
            clickhouse_queries.bytes_scanned,
            clickhouse_queries.duration_ms,
            clickhouse_queries.stats
        FROM {table}
        WHERE {where_sql}
        ORDER BY timestamp DESC
        LIMIT {int(req.max_rows)}
    """


def _infer_request_class(body: dict[str, Any]) -> type[ProtobufMessage] | None:
    """Pick a protobuf request type from the shape of the stored JSON body.

    Order matters: TimeSeriesRequest and TraceItemTableRequest both may carry
    ``group_by``/``groupBy``, so timeseries-only markers must win first.
    """
    if (
        "expressions" in body
        or "granularitySecs" in body
        or "granularity_secs" in body
        or "aggregations" in body
    ):
        return TimeSeriesRequest
    if "statsTypes" in body or "stats_types" in body:
        return TraceItemStatsRequest
    if "columns" in body or "groupBy" in body or "group_by" in body:
        return TraceItemTableRequest
    for key in ("request", "body"):
        nested = body.get(key)
        if isinstance(nested, dict):
            inferred = _infer_request_class(nested)
            if inferred is not None:
                return inferred
    return None


def _parse_request_body(raw_body: str) -> ProtobufMessage | None:
    if not raw_body:
        return None
    try:
        body = json.loads(raw_body)
    except (TypeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None

    request_class = _infer_request_class(body)
    if request_class is None:
        return None

    try:
        return Parse(
            json.dumps(body),
            request_class(),
            ignore_unknown_fields=True,
        )
    except Exception:
        logger.debug("Failed to parse EAP request body into %s", request_class.__name__)
        return None


def _query_info_from_stats(stats_list: list[Any] | None) -> dict[str, str] | None:
    """Prefer query_info already embedded in routing stats when present."""
    if not stats_list:
        return None
    for entry in stats_list:
        if entry is None:
            continue
        try:
            parsed = json.loads(entry) if isinstance(entry, str) else entry
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, dict):
            continue
        query_info = parsed.get("query_info")
        if isinstance(query_info, dict) and query_info.get("query_type"):
            return {str(k): str(v) for k, v in query_info.items()}
    return None


def _sum_array(values: list[Any] | None) -> int:
    if not values:
        return 0
    total = 0
    for v in values:
        try:
            total += int(v or 0)
        except (TypeError, ValueError):
            continue
    return total


def _collect_query_ids(values: list[Any] | None) -> list[str]:
    if not values:
        return []
    ids: list[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in ("0" * 32, "00000000-0000-0000-0000-000000000000"):
            ids.append(s)
    return ids


def _eap_profile_cluster_name() -> str | None:
    """Resolved ClickHouse cluster name for EAP ProfileEvents lookups."""
    try:
        cluster = get_storage(StorageKey(_EAP_PROFILE_STORAGE)).get_cluster()
        return cluster.get_clickhouse_cluster_name()
    except Exception as e:
        logger.warning("Failed to resolve EAP cluster name for ProfileEvents: %s", e)
        return None


def _fetch_profile_events(query_ids: list[str], hours: int) -> dict[str, dict[str, int]]:
    """Batch-fetch ProfileEvents from system.query_log on the EAP cluster.

    Returns mapping of normalized query_id -> {ProfileEventName: value}.
    Failures are swallowed so the page still works from querylog-only data.
    """
    if not query_ids:
        return {}

    capped = query_ids[:_MAX_PROFILE_QUERY_IDS]
    normalized = list({qid.replace("-", "") for qid in capped})
    dashed: list[str] = []
    for qid in capped:
        raw = qid.replace("-", "")
        if len(raw) == 32:
            dashed.append(f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}")
    all_ids = list({*normalized, *dashed})
    id_list = ", ".join(f"'{_escape_literal(qid)}'" for qid in all_ids)

    cluster_name = _eap_profile_cluster_name()
    sql_cluster = None
    if cluster_name:
        sql_cluster = f"""
            SELECT
                replaceAll(toString(query_id), '-', '') AS qid,
                ProfileEvents
            FROM clusterAllReplicas('{_escape_literal(cluster_name)}', system.query_log)
            WHERE type = 'QueryFinish'
              AND event_time > now() - INTERVAL {int(hours)} HOUR
              AND replaceAll(toString(query_id), '-', '') IN ({id_list})
            SETTINGS skip_unavailable_shards = 1, max_threads = 0
        """
    sql_local = f"""
        SELECT
            replaceAll(toString(query_id), '-', '') AS qid,
            ProfileEvents
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND event_time > now() - INTERVAL {int(hours)} HOUR
          AND replaceAll(toString(query_id), '-', '') IN ({id_list})
        SETTINGS max_threads = 0
    """

    try:
        connection: ClickhousePool = get_ro_query_node_connection(
            _EAP_PROFILE_STORAGE, ClickhouseClientSettings.QUERY
        )
        if sql_cluster is not None:
            try:
                result = connection.execute(query=sql_cluster, with_column_types=True)
            except Exception as e:
                logger.warning(
                    "clusterAllReplicas ProfileEvents lookup failed, trying local: %s", e
                )
                result = connection.execute(query=sql_local, with_column_types=True)
        else:
            result = connection.execute(query=sql_local, with_column_types=True)
    except Exception as e:
        logger.warning("ProfileEvents lookup failed: %s", e)
        return {}

    out: dict[str, dict[str, int]] = {}
    for row in result.results or []:
        qid, events = row[0], row[1]
        if not qid or not isinstance(events, dict):
            continue
        key = str(qid).replace("-", "")
        bucket = out.setdefault(key, {})
        for pe_name, pe_val in events.items():
            try:
                bucket[str(pe_name)] = bucket.get(str(pe_name), 0) + int(pe_val or 0)
            except (TypeError, ValueError):
                continue
    return out


def _top_n(counts: dict[str, int], n: int = 8) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:n])


@dataclass
class _Accumulator:
    query_count: int = 0
    queries_profiled: int = 0
    bytes_profiled: int = 0
    resources: ResourceTotals = field(default_factory=ResourceTotals)
    by_trace_item_type: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    by_filter_profile: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    by_referrer: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    examples: list[dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        *,
        resources: ResourceTotals,
        profiled: bool,
        trace_item_type: str,
        filter_profile: str,
        referrer: str,
        example: dict[str, Any] | None,
    ) -> None:
        self.query_count += 1
        self.resources.add_other(resources)
        if profiled:
            self.queries_profiled += 1
            self.bytes_profiled += resources.bytes_scanned
        self.by_trace_item_type[trace_item_type] += 1
        self.by_filter_profile[filter_profile] += 1
        if referrer:
            self.by_referrer[referrer] += 1
        if example is not None and len(self.examples) < _MAX_EXAMPLES_PER_TYPE:
            self.examples.append(example)


def _pct(part: int | float, whole: int | float) -> float:
    if not whole:
        return 0.0
    return 100.0 * float(part) / float(whole)


def _finalize_buckets(
    by_type: dict[str, _Accumulator],
    totals: ResourceTotals,
    total_queries: int,
) -> list[QueryTypeBucket]:
    buckets: list[QueryTypeBucket] = []
    for query_type, acc in by_type.items():
        r = acc.resources
        buckets.append(
            QueryTypeBucket(
                query_type=query_type,
                query_count=acc.query_count,
                queries_profiled=acc.queries_profiled,
                resources=r,
                avg_bytes_scanned=(r.bytes_scanned / acc.query_count if acc.query_count else 0.0),
                avg_duration_ms=(r.duration_ms / acc.query_count if acc.query_count else 0.0),
                pct_of_bytes=_pct(r.bytes_scanned, totals.bytes_scanned),
                pct_of_queries=_pct(acc.query_count, total_queries),
                pct_of_cpu=_pct(r.cpu_total_us, totals.cpu_total_us),
                pct_of_memory_peak=_pct(r.memory_peak_bytes, totals.memory_peak_bytes),
                pct_of_io=_pct(r.io_total_bytes, totals.io_total_bytes),
                pct_of_network=_pct(r.network_total_bytes, totals.network_total_bytes),
                pct_queries_profiled=_pct(acc.queries_profiled, acc.query_count),
                pct_bytes_profiled=_pct(acc.bytes_profiled, r.bytes_scanned),
                by_trace_item_type=_top_n(dict(acc.by_trace_item_type)),
                by_filter_profile=_top_n(dict(acc.by_filter_profile)),
                by_referrer=_top_n(dict(acc.by_referrer)),
                examples=acc.examples,
            )
        )
    buckets.sort(key=lambda b: b.resources.bytes_scanned, reverse=True)
    return buckets


def _finalize_dimension(
    counts_bytes: dict[str, int],
    counts_queries: dict[str, int],
    counts_cpu: dict[str, int],
    total_bytes: int,
    total_cpu: int,
) -> list[dict[str, Any]]:
    keys = set(counts_bytes) | set(counts_queries) | set(counts_cpu)
    rows: list[dict[str, Any]] = []
    for key in keys:
        b = counts_bytes.get(key, 0)
        c = counts_queries.get(key, 0)
        cpu = counts_cpu.get(key, 0)
        rows.append(
            {
                "key": key,
                "query_count": c,
                "total_bytes_scanned": b,
                "total_cpu_us": cpu,
                "pct_of_bytes": _pct(b, total_bytes),
                "pct_of_cpu": _pct(cpu, total_cpu),
            }
        )
    rows.sort(key=lambda r: int(r["total_bytes_scanned"]), reverse=True)
    return rows[:20]


def analyze_eap_queries(req: EapQueryAnalysisRequest, user: str) -> EapQueryAnalysisResult:
    """Fetch, categorize, and aggregate EAP querylog rows for the admin page.

    Uses the audited querylog runner for the primary scan. ProfileEvents
    enrichment is best-effort against the EAP cluster's system.query_log.
    """
    sql = _build_fetch_sql(req)
    # run_querylog_query is @audit_log'd — records the user + SQL.
    # max_threads=0 lets ClickHouse use all cores for whole-dataset scans.
    result = run_querylog_query(sql, user, max_threads=_EAP_STATS_MAX_THREADS)
    raw_rows = result.results or []

    parsed_rows: list[dict[str, Any]] = []
    all_query_ids: list[str] = []

    for row in raw_rows:
        (
            request_id,
            timestamp,
            referrer,
            dataset,
            organization,
            duration_ms,
            status,
            request_body,
            query_id_arr,
            bytes_scanned_arr,
            ch_duration_arr,
            stats_arr,
        ) = row

        bytes_scanned = _sum_array(bytes_scanned_arr)
        ch_duration = _sum_array(ch_duration_arr)
        duration = ch_duration or int(duration_ms or 0)
        referrer_s = str(referrer or "")
        query_ids = _collect_query_ids(query_id_arr)
        all_query_ids.extend(query_ids)

        query_info = _query_info_from_stats(stats_arr)
        categorized = True
        if query_info is None:
            message = _parse_request_body(request_body or "")
            if message is None:
                categorized = False
                query_info = {
                    "query_type": "uncategorized",
                    "trace_item_type": "unknown",
                    "filter_profile": "none",
                    "has_groupby": "false",
                    "groupby_count": "0",
                    "has_formula": "false",
                    "has_cross_item": "false",
                }
            else:
                query_info = extract_query_info(message)

        parsed_rows.append(
            {
                "request_id": str(request_id),
                "timestamp": str(timestamp),
                "referrer": referrer_s,
                "dataset": str(dataset or ""),
                "organization": organization,
                "status": str(status or ""),
                "bytes_scanned": bytes_scanned,
                "duration_ms": duration,
                "query_ids": query_ids,
                "query_info": query_info,
                "categorized": categorized,
            }
        )

    profile_by_qid: dict[str, dict[str, int]] = {}
    unique_ids: list[str] = []
    query_ids_capped = False
    if req.include_profile_events and all_query_ids:
        seen: set[str] = set()
        for qid in all_query_ids:
            key = qid.replace("-", "")
            if key not in seen:
                seen.add(key)
                unique_ids.append(qid)
        query_ids_capped = len(unique_ids) > _MAX_PROFILE_QUERY_IDS
        profile_by_qid = _fetch_profile_events(unique_ids, req.hours)

    by_type: dict[str, _Accumulator] = defaultdict(_Accumulator)
    filter_bytes: dict[str, int] = defaultdict(int)
    filter_counts: dict[str, int] = defaultdict(int)
    filter_cpu: dict[str, int] = defaultdict(int)
    item_bytes: dict[str, int] = defaultdict(int)
    item_counts: dict[str, int] = defaultdict(int)
    item_cpu: dict[str, int] = defaultdict(int)
    referrer_bytes: dict[str, int] = defaultdict(int)
    referrer_counts: dict[str, int] = defaultdict(int)
    referrer_cpu: dict[str, int] = defaultdict(int)

    totals = ResourceTotals()
    rows_categorized = 0
    rows_failed = 0
    queries_with_query_id = 0
    queries_profiled = 0
    bytes_profiled = 0
    duration_ms_profiled = 0

    for prow in parsed_rows:
        resources = ResourceTotals(
            bytes_scanned=int(prow["bytes_scanned"]),
            duration_ms=int(prow["duration_ms"]),
        )
        request_profiled = False
        if prow["query_ids"]:
            queries_with_query_id += 1
        for qid in prow["query_ids"]:
            events = profile_by_qid.get(str(qid).replace("-", ""))
            if events:
                resources.add_profile_events(events)
                request_profiled = True

        if request_profiled:
            queries_profiled += 1
            bytes_profiled += resources.bytes_scanned
            duration_ms_profiled += resources.duration_ms

        totals.add_other(resources)

        query_info = prow["query_info"]
        query_type = query_info.get("query_type", "uncategorized")
        trace_item_type = query_info.get("trace_item_type", "unknown")
        filter_profile = query_info.get("filter_profile", "none")
        referrer_s = prow["referrer"]

        if prow["categorized"]:
            rows_categorized += 1
        else:
            rows_failed += 1

        cpu = resources.cpu_total_us

        example = {
            "request_id": prow["request_id"],
            "timestamp": prow["timestamp"],
            "referrer": referrer_s,
            "dataset": prow["dataset"],
            "organization": prow["organization"],
            "bytes_scanned": resources.bytes_scanned,
            "duration_ms": resources.duration_ms,
            "cpu_us": cpu,
            "memory_peak_bytes": resources.memory_peak_bytes,
            "io_selected_bytes": resources.io_selected_bytes,
            "network_receive_bytes": resources.network_receive_bytes,
            "profiled": request_profiled,
            "status": prow["status"],
            "query_info": query_info,
        }

        by_type[query_type].add(
            resources=resources,
            profiled=request_profiled,
            trace_item_type=trace_item_type,
            filter_profile=filter_profile,
            referrer=referrer_s,
            example=example,
        )

        filter_bytes[filter_profile] += resources.bytes_scanned
        filter_counts[filter_profile] += 1
        filter_cpu[filter_profile] += cpu
        item_bytes[trace_item_type] += resources.bytes_scanned
        item_counts[trace_item_type] += 1
        item_cpu[trace_item_type] += cpu
        if referrer_s:
            referrer_bytes[referrer_s] += resources.bytes_scanned
            referrer_counts[referrer_s] += 1
            referrer_cpu[referrer_s] += cpu

    total_queries = len(parsed_rows)
    total_cpu = totals.cpu_total_us
    query_ids_sampled = len({qid.replace("-", "") for qid in all_query_ids})
    query_ids_looked_up = (
        min(query_ids_sampled, _MAX_PROFILE_QUERY_IDS) if req.include_profile_events else 0
    )
    query_ids_matched = len(profile_by_qid)

    coverage = ProfileCoverage(
        enabled=req.include_profile_events,
        queries_total=total_queries,
        queries_with_query_id=queries_with_query_id,
        queries_profiled=queries_profiled,
        query_ids_sampled=query_ids_sampled,
        query_ids_looked_up=query_ids_looked_up,
        query_ids_matched=query_ids_matched,
        query_ids_capped=query_ids_capped,
        bytes_total=totals.bytes_scanned,
        bytes_profiled=bytes_profiled,
        duration_ms_total=totals.duration_ms,
        duration_ms_profiled=duration_ms_profiled,
        pct_queries_profiled=_pct(queries_profiled, total_queries),
        pct_queries_with_query_id_profiled=_pct(queries_profiled, queries_with_query_id),
        pct_query_ids_matched=_pct(query_ids_matched, query_ids_looked_up),
        pct_bytes_profiled=_pct(bytes_profiled, totals.bytes_scanned),
        pct_duration_profiled=_pct(duration_ms_profiled, totals.duration_ms),
    )

    return EapQueryAnalysisResult(
        hours=req.hours,
        max_rows=req.max_rows,
        rows_scanned=total_queries,
        rows_categorized=rows_categorized,
        rows_failed=rows_failed,
        profile_events_enabled=req.include_profile_events,
        profile_events_matched=totals.profile_events_matched,
        profile_coverage=coverage,
        total_resources=totals,
        by_query_type=_finalize_buckets(by_type, totals, total_queries),
        by_filter_profile=_finalize_dimension(
            filter_bytes, filter_counts, filter_cpu, totals.bytes_scanned, total_cpu
        ),
        by_trace_item_type=_finalize_dimension(
            item_bytes, item_counts, item_cpu, totals.bytes_scanned, total_cpu
        ),
        by_referrer=_finalize_dimension(
            referrer_bytes, referrer_counts, referrer_cpu, totals.bytes_scanned, total_cpu
        ),
    )


def result_to_dict(result: EapQueryAnalysisResult) -> dict[str, Any]:
    return asdict(result)
