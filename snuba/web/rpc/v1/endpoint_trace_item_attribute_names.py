import re
import uuid
from collections.abc import Mapping
from typing import Any

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, not_cond, or_cond
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.reader import Row
from snuba.request import Request as SnubaRequest
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    as_datetime,
    next_monday,
    prev_monday,
    project_id_and_org_conditions,
    semver_sort_key,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import extract_response_meta
from snuba.web.rpc.proto_visitor import ProtoVisitor, TraceItemFilterWrapper
from snuba.web.rpc.v1.resolvers.R_eap_items import co_occurring_attrs
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import CoOccurringAttrsSource

# max value the user can provide for 'limit' in their request
MAX_REQUEST_LIMIT = 1000
UNSEARCHABLE_ATTRIBUTE_KEYS = [
    "sentry.event_id",
    "sentry.segment_id",
    "sentry.start_timestamp_precise",
    "sentry.received",
    "sentry.is_segment",
    "sentry.exclusive_time_ms",
    "sentry.end_timestamp_precise",
]

NON_STORED_ATTRIBUTE_KEYS = ["sentry.service"]
MATCH_MODES = {
    TraceItemAttributeNamesRequest.MatchMode.MATCH_MODE_ANY: f.hasAny,
    TraceItemAttributeNamesRequest.MatchMode.MATCH_MODE_ALL: f.hasAll,
}


def _order_by_count(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller opted into frequency ordering via ``order_by`` (sort:-count()).

    When ``order_by`` is unset, ``column`` defaults to COLUMN_UNSPECIFIED, so the
    endpoint keeps its historical name-ascending ordering and existing consumers
    are unaffected.
    """
    return request.order_by.column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT


def _effective_order_by_column(
    request: TraceItemAttributeNamesRequest,
    source: CoOccurringAttrsSource,
) -> TraceItemAttributeNamesRequest.OrderBy.Column.ValueType:
    """The ordering the query will apply, which may differ from the one requested.

    Only v2 records ``last_seen``, so a recency request that lands on v1 degrades to frequency
    ordering rather than failing: both rank "attributes worth showing first", so an
    autocomplete caller still gets a useful answer. It stays detectable because ``last_seen``
    is then absent from the response, and via a metric.

    Everything downstream keys off this rather than ``request.order_by.column``, so the
    ClickHouse ORDER BY and the Python re-sort cannot disagree about which ordering was used.
    """
    column = request.order_by.column
    if (
        column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
        and not source.has_last_seen
    ):
        return TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT
    return column


def _aggregates_attributes(
    order_by_column: TraceItemAttributeNamesRequest.OrderBy.Column.ValueType,
) -> bool:
    """Whether an ordering groups the keys, and so can report ``count``/``last_seen``.

    Name ordering instead selects distinct keys and reports neither.
    """
    return order_by_column in (
        TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT,
        TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN,
    )


def _order_by_semver(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller requested SORT_SEMVER (semver) ordering of names."""
    return request.order_by.sort == TraceItemAttributeNamesRequest.OrderBy.SORT_SEMVER


def _order_by_name_descending(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller requested name ordering in descending order.

    Both an explicit ``COLUMN_NAME`` and ``SORT_SEMVER`` (which orders by the
    semver key of the name, typically with ``column`` left unset) select name
    ordering, so ``descending`` flips either. Unset ordering stays name-ascending
    for backwards compatibility.
    """
    return request.order_by.descending and (
        request.order_by.column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_NAME
        or _order_by_semver(request)
    )


_SEMVER_NUMERIC_RE = re.compile(r"^[0-9]+(\.[0-9]+)*$")


def _semver_sort_key_py(name: str) -> tuple[tuple[int, int, int, int], int, str]:
    """Python mirror of common.semver_sort_key, used to re-sort names in Python so
    the merged (ClickHouse + synthetic) result matches the ClickHouse ORDER BY."""
    non_null = name or ""
    version_no_prefix = non_null.split("@")[-1]
    # Drop SemVer build metadata ("1.2.3+build") before parsing; it must not
    # affect precedence (mirrors common.semver_sort_key).
    version_no_build = version_no_prefix.split("+")[0]
    release_part = version_no_build.split("-")[0]
    # Mirror ClickHouse toUInt32OrZero: only ASCII decimal parses, anything else
    # (including Unicode digits like "²", where str.isdigit() is True but int()
    # raises) maps to 0.
    components = [int(c) if (c.isascii() and c.isdigit()) else 0 for c in release_part.split(".")]
    components = (components + [0, 0, 0, 0])[:4]
    is_stable = 1 if _SEMVER_NUMERIC_RE.match(version_no_build) else 0
    return (
        (components[0], components[1], components[2], components[3]),
        is_stable,
        non_null,
    )


def _name_order_by_expression(semver: bool) -> Expression:
    """ClickHouse ORDER BY expression for the attribute name.

    Default orders by the raw (type, name) tuple; SORT_SEMVER orders by the
    semver key of the name part so versions sort numerically.
    """
    if semver:
        return semver_sort_key(f.tupleElement(column("attr_key"), 2))
    return column("attr_key")


class AttributeKeyCollector(ProtoVisitor):
    def __init__(self) -> None:
        self.keys: set[str] = set()

    def visit_TraceItemFilterWrapper(
        self, trace_item_filter_wrapper: TraceItemFilterWrapper
    ) -> None:
        trace_item_filter = trace_item_filter_wrapper.underlying_proto
        if trace_item_filter.HasField("exists_filter"):
            self.keys.add(trace_item_filter.exists_filter.key.name)
        elif trace_item_filter.HasField("comparison_filter"):
            self.keys.add(trace_item_filter.comparison_filter.key.name)


def convert_to_attributes(
    query_res: QueryResult, attribute_type: AttributeKey.Type.ValueType
) -> list[TraceItemAttributeNamesResponse.Attribute]:
    def t(row: Row) -> TraceItemAttributeNamesResponse.Attribute:
        # our query to snuba only selected 1 column, attr_key
        # so the result should only have 1 item per row
        vals = row.values()
        assert len(vals) == 1
        attr_name = list(vals)[0]
        return TraceItemAttributeNamesResponse.Attribute(name=attr_name, type=attribute_type)

    return list(map(t, query_res.result["data"]))


def get_co_occurring_attributes_date_condition(
    request: TraceItemAttributeNamesRequest,
) -> Expression:
    # round the lower timestamp to the previous monday
    lower_ts = request.meta.start_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)
    lower_ts = prev_monday(lower_ts)

    # round the upper timestamp to the next monday
    upper_ts = request.meta.end_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)
    upper_ts = next_monday(upper_ts)

    return and_cond(
        f.less(
            column("date"),
            f.toDate(upper_ts),
        ),
        f.greaterOrEquals(
            column("date"),
            f.toDate(lower_ts),
        ),
    )


def _add_substring_match_optimization(
    request: TraceItemAttributeNamesRequest,
    condition: Expression,
    *,
    source: CoOccurringAttrsSource,
) -> FunctionCall | Expression:
    """Add arrayExists to WHERE clause to filter rows before loading arrays.

    This reduces memory usage by only processing rows with matching attributes.
    Similar to the hasAll optimization, this allows ClickHouse to use PREWHERE
    and filter rows before loading large attribute arrays.
    """
    if not request.value_substring_match:
        return condition

    pattern = f"%{request.value_substring_match}%"
    like_lambda = Lambda(None, ("x",), f.like(Argument(None, "x"), pattern))

    exists = [
        f.arrayExists(like_lambda, column(col)) for col in source.key_array_columns(request.type)
    ]
    if not exists:
        return condition
    if len(exists) == 1:
        return and_cond(condition, exists[0])
    return and_cond(condition, or_cond(*exists))


def get_co_occurring_attributes(
    request: TraceItemAttributeNamesRequest,
    source: CoOccurringAttrsSource | None = None,
) -> SnubaRequest:
    """Constructs the clickhouse query for co-occurring attributes:


      The query at the end looks something like this:

          -- Default ordering (order_by unset or COLUMN_NAME): distinct keys by name
          SELECT distinct(arrayJoin(arrayFilter(attr -> ((NOT has(['test_tag_1_0'], attr.2)) AND startsWith(attr.2, 'test_')), arrayMap(x -> ('TYPE_STRING', x), attributes_string)))) AS attr_key
          FROM eap_item_co_occurring_attrs_1_local
          WHERE (item_type = 1) AND (project_id IN [1]) AND (organization_id = 1) AND (date < toDateTime(toDate('2025-03-17', 'Universal'))) AND (date >= toDateTime(toDate('2025-03-10', 'Universal')))

          -- This is a faster way of looking up whether all attributes co-exist, it uses an array of hashes. This avoids string equality comparisons
          AND hasAll(attribute_keys_hash, [cityHash64('test_tag_1_0')])
          --

          ORDER BY attr_key ASC
          LIMIT 10000

          -- COLUMN_COUNT and COLUMN_LAST_SEEN instead group the keys and aggregate, most
          -- common or most recent first:
          --   SELECT arrayJoin(...) AS attr_key, <count aggregate> AS count,
          --          max(last_seen) AS last_seen
          --   ... GROUP BY attr_key ORDER BY <count|last_seen> DESC, attr_key ASC

      **Storage:** the roll-up this reads and the parts of the query shape that differ between
      the two (per-type key arrays, the aggregates) come from the `CoOccurringAttrsSource`
      returned by `resolvers.R_eap_items.co_occurring_attrs.for_request`.

      **Explanation:**

      1. This query would narrow down the granules to scan using the primary key (stored in memory):

          `(organization_id, project_id, date, item_type, key_val_hash)`

      2. The following line checks to see that the events contain all of the co-occurring attributes
          `hasAll(attribute_keys_hash, [cityHash64('test_tag_1_0')])`

          - This hits the bloom filter index on `attribute_keys_hash` and prevents granules that do not have all of the co-occurring attributes from being scanned
          - loading `attributes_string_hash` is orders of magnitude faster than the `attributes_string` array because all elements are fixed size (UInt64) and
              equality can be checked in a single CPU instruction (or fewer if SIMD instructions are used)
          - Clickhouse automatically puts this clause into the [PREWHERE](https://clickhouse.com/docs/sql-reference/statements/select/prewhere)
      3. The inner query surfaces all co-occurring attributes with `allocation_policy.is_throttled` on an event-by-event basis, stopping at 1000 attributes

      ```sql
      -- each of the co-occurring attributes becomes a row sent to the outer query
      arrayJoin(
              arrayFilter(
                  attr -> NOT has(['test_tag_1_0'], attr),
                  attributes_string
              )
      ) AS attr_key
      ```
    4. . The outer query returns the co-occurring attribute keys. By default they are
       deduplicated and ordered by name; when COLUMN_COUNT ordering is requested they
       are grouped, counted, and ordered by frequency (most common first).

      **The following things make this query more performant than searching the source table:**

          - The attribute keys are NOT bucketed. Since the functionality has to process ALL the attributes, all the bucket files would have to be opened for each granule.
          This way Clickhouse only has to open 1 file
          - The attribute keys are deduplicated, resulting in less data to scan (~95% row reduction rate)
          - there is a bloom filter index on all key values
    """
    # Callers that need the source themselves must pass the one they resolved: resolving reads
    # runtime options, and an option flipping between two reads would build the query for one
    # storage while the response is processed as if it were the other.
    if source is None:
        source = co_occurring_attrs.for_request(request)
    order_by_column = _effective_order_by_column(request, source)

    # get all attribute keys from the filter
    collector = AttributeKeyCollector()
    TraceItemFilterWrapper(request.intersecting_attributes_filter).accept(collector)
    attribute_keys_to_search = collector.keys

    condition: Expression = and_cond(
        project_id_and_org_conditions(request.meta),
        get_co_occurring_attributes_date_condition(request),
    )

    if attribute_keys_to_search:
        condition = and_cond(
            condition,
            MATCH_MODES.get(request.match_mode, f.hasAll)(
                column("attribute_keys_hash"),
                f.array(*[f.cityHash64(k) for k in attribute_keys_to_search]),
            ),
        )

    # Optimization: Add arrayExists to WHERE clause to filter rows before loading arrays
    condition = _add_substring_match_optimization(request, condition, source=source)

    if request.meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
        condition = and_cond(f.equals(column("item_type"), request.meta.trace_item_type), condition)

    # One (type, key) tuple array per column read, so every key carries the AttributeKey type
    # of the column it came from.
    typed_arrays = [
        f.arrayMap(
            Lambda(
                None,
                ("x",),
                f.tuple(
                    type_name,
                    column("x"),
                ),
            ),
            column(col),
        )
        for col, type_name in source.typed_key_arrays(request.type)
    ]
    array_func = typed_arrays[0] if len(typed_arrays) == 1 else f.arrayConcat(*typed_arrays)

    # Exclude the unsearchable keys with NOT has(array(...), x) rather than
    # NOT (x IN (...)). A constant IN-set makes ClickHouse build an internal
    # prepared set whose server-generated identifier (__set_String_<hash>_<hash>)
    # is baked into the result-block column name. Because this filter lives inside
    # the arrayJoin'd SELECT expression, that name is matched by string across
    # `Remote`; on a mixed-version cluster the two sides hash the set differently,
    # so the names disagree and distributed reads fail with
    # "Code: 10 ... Not found column ... While executing Remote." (SNUBA-B82).
    # has() over a constant array keeps the array inline in the column name, which
    # is byte-stable across versions. The set here is tiny so there's no perf cost.
    attr_filter = not_cond(
        f.has(
            f.array(*UNSEARCHABLE_ATTRIBUTE_KEYS),
            f.tupleElement(column("attr"), 2),
        )
    )
    if request.value_substring_match:
        attr_filter = and_cond(
            attr_filter,
            f.like(f.tupleElement(column("attr"), 2), f"%{request.value_substring_match}%"),
        )

    attr_key_expression = f.arrayJoin(
        f.arrayFilter(
            Lambda(None, ("attr",), attr_filter),
            array_func,
        ),
        alias="attr_key",
    )

    semver = _order_by_semver(request)
    if _aggregates_attributes(order_by_column):
        selected_columns = [
            SelectedExpression(name="attr_key", expression=attr_key_expression),
            SelectedExpression(name="count", expression=source.count_expression()),
        ]
        if source.has_last_seen:
            # Selected whenever the storage has it, so a caller ordering by frequency still
            # learns how recent each attribute is.
            selected_columns.append(
                SelectedExpression(name="last_seen", expression=source.last_seen_expression())
            )
        groupby: list[Expression] | None = [column("attr_key")]
        order_by = [
            OrderBy(
                direction=(
                    OrderByDirection.DESC if request.order_by.descending else OrderByDirection.ASC
                ),
                expression=column(
                    "last_seen"
                    if order_by_column
                    == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
                    else "count"
                ),
            ),
            # stable tiebreak for keys with the same frequency/recency (semver key when
            # SORT_SEMVER was requested)
            OrderBy(direction=OrderByDirection.ASC, expression=_name_order_by_expression(semver)),
        ]
    else:
        # Default (order_by unset or COLUMN_NAME): distinct keys ordered by name.
        # Unspecified ordering keeps the historical name-ascending result so that
        # existing consumers are unaffected.
        name_descending = _order_by_name_descending(request)
        selected_columns = [
            SelectedExpression(name="attr_key", expression=f.distinct(attr_key_expression)),
        ]
        groupby = None
        order_by = [
            OrderBy(
                direction=OrderByDirection.DESC if name_descending else OrderByDirection.ASC,
                expression=_name_order_by_expression(semver),
            ),
        ]

    query = Query(
        from_clause=source.data_source,
        selected_columns=selected_columns,
        groupby=groupby,
        condition=condition,
        order_by=order_by,
        # chosen arbitrarily to be a high number
        limit=request.limit,
        offset=request.page_token.offset if request.page_token.HasField("offset") else 0,
    )

    treeify_or_and_conditions(query)
    settings = HTTPQuerySettings()
    snuba_request = SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=query,
        query_settings=settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api=EndpointTraceItemAttributeNames.config_key(),
        ),
    )
    return snuba_request


def _aggregate_sort_key(row: Mapping[str, Any], sort_column: str) -> float:
    """Comparable value for the aggregate the ClickHouse ORDER BY used.

    A number, so counts and timestamps share one code path and rows missing the column (the
    synthetic non-stored keys) compare lowest instead of raising on a mixed-type comparison.
    """
    value = row.get(sort_column)
    if value is None:
        return 0.0
    if sort_column == "last_seen":
        return as_datetime(value).timestamp()
    return float(value)


def convert_co_occurring_results_to_attributes(
    request: TraceItemAttributeNamesRequest,
    query_res: QueryResult,
    order_by_column: TraceItemAttributeNamesRequest.OrderBy.Column.ValueType | None = None,
) -> list[TraceItemAttributeNamesResponse.Attribute]:
    """Build the response attributes, re-sorting to match the ClickHouse ORDER BY.

    ``order_by_column`` must be the value the query was built with (see
    ``_effective_order_by_column``), or the merge below re-sorts into a different order than
    ClickHouse used. Defaults to the requested column.
    """
    if order_by_column is None:
        order_by_column = request.order_by.column

    def t(row: Row) -> TraceItemAttributeNamesResponse.Attribute:
        attr_type, attr_name = row["attr_key"]
        assert isinstance(attr_type, str)
        attribute = TraceItemAttributeNamesResponse.Attribute(
            name=attr_name, type=getattr(AttributeKey.Type, attr_type)
        )
        # Only selected on the aggregating orderings; the synthetic non-stored keys have
        # neither, and `last_seen` is absent on storages that do not record it.
        count = row.get("count")
        if count is not None:
            attribute.count = int(count)
        last_seen = row.get("last_seen")
        if last_seen is not None:
            attribute.last_seen.FromDatetime(as_datetime(last_seen))
        return attribute

    # Name-ordering key that mirrors the ClickHouse ORDER BY: the raw (type, name)
    # tuple by default, or the semver key of the name under SORT_SEMVER.
    semver = _order_by_semver(request)

    def _name_key(row: Mapping[str, Any]) -> Any:
        attr_key = row.get("attr_key", ("TYPE_STRING", ""))
        attr_type, attr_name = attr_key[0], attr_key[1]
        if semver:
            return _semver_sort_key_py(attr_name)
        return (attr_type, attr_name)

    data = query_res.result.get("data", [])
    if request.type in (AttributeKey.TYPE_UNSPECIFIED, AttributeKey.TYPE_STRING):
        non_stored = [
            {"attr_key": ("TYPE_STRING", key_name)}
            for key_name in NON_STORED_ATTRIBUTE_KEYS
            if request.value_substring_match in key_name
        ]
        non_stored.sort(key=_name_key)
        if _aggregates_attributes(order_by_column):
            # Match ClickHouse: the aggregate in the requested direction, then name ASC
            # (two stable passes). Synthetic non-stored keys have no aggregate value, so
            # pin them first rather than relying on a sentinel.
            sort_column = (
                "last_seen"
                if order_by_column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
                else "count"
            )
            data.sort(key=_name_key)
            data.sort(
                key=lambda row: _aggregate_sort_key(row, sort_column),
                reverse=request.order_by.descending,
            )
            data = non_stored + data
        else:
            # Merge synthetic non-stored keys in and re-sort by name in the
            # requested direction, matching the ClickHouse ORDER BY.
            data.extend(non_stored)
            data.sort(
                key=_name_key,
                reverse=_order_by_name_descending(request),
            )

    return list(map(t, data))


class EndpointTraceItemAttributeNames(
    RPCEndpoint[TraceItemAttributeNamesRequest, TraceItemAttributeNamesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> type[TraceItemAttributeNamesRequest]:
        return TraceItemAttributeNamesRequest

    @classmethod
    def response_class(cls) -> type[TraceItemAttributeNamesResponse]:
        return TraceItemAttributeNamesResponse

    def _build_response(
        self,
        req: TraceItemAttributeNamesRequest,
        res: QueryResult,
    ) -> TraceItemAttributeNamesResponse:
        attributes = convert_to_attributes(res, req.type)
        page_token = (
            PageToken(offset=req.page_token.offset + len(attributes))
            if req.page_token.HasField("offset") or len(attributes) == 0
            else PageToken(
                filter_offset=TraceItemFilter(
                    comparison_filter=ComparisonFilter(
                        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="attr_key"),
                        op=ComparisonFilter.OP_GREATER_THAN,
                        value=AttributeValue(val_str=attributes[-1].name),
                    )
                )
            )
        )
        return TraceItemAttributeNamesResponse(
            attributes=attributes,
            page_token=page_token,
            meta=extract_response_meta(req.meta.request_id, req.meta.debug, [res], [self._timer]),
        )

    def _execute(self, in_msg: TraceItemAttributeNamesRequest) -> TraceItemAttributeNamesResponse:
        # Resolved once and shared, so the query and the response re-sort cannot disagree.
        source = co_occurring_attrs.for_request(in_msg)
        order_by_column = _effective_order_by_column(in_msg, source)
        if order_by_column != in_msg.order_by.column:
            # Keeps an otherwise silent downgrade visible during the v2 rollout.
            self.metrics.increment(
                "attribute_names_order_by_degraded",
                1,
                tags={
                    "requested": TraceItemAttributeNamesRequest.OrderBy.Column.Name(
                        in_msg.order_by.column
                    ),
                    "applied": TraceItemAttributeNamesRequest.OrderBy.Column.Name(order_by_column),
                    "storage": source.storage_key.value,
                },
            )

        snuba_request = get_co_occurring_attributes(in_msg, source)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )

        response = TraceItemAttributeNamesResponse(
            attributes=convert_co_occurring_results_to_attributes(in_msg, res, order_by_column),
            meta=extract_response_meta(
                in_msg.meta.request_id, in_msg.meta.debug, [res], [self._timer]
            ),
        )
        return response
