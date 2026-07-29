import re
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
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
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Storage
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column, not_cond, or_cond
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.reader import Row
from snuba.request import Request as SnubaRequest
from snuba.state.sentry_options import get_option
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.common import (
    next_monday,
    prev_monday,
    project_id_and_org_conditions,
    semver_sort_key,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import extract_response_meta
from snuba.web.rpc.proto_visitor import ProtoVisitor, TraceItemFilterWrapper

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

# The two co-occurring-attribute storages this endpoint can read. v1 is a
# ReplacingMergeTree holding one row per distinct attribute-key set with only
# string/float/bool key arrays; v2 is a SummingMergeTree that additionally carries an
# occurrence `count` and one key array per attribute type (int and the four array types).
CO_OCCURRING_ATTRS_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs")
CO_OCCURRING_ATTRS_V2_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs_v2")

# Killswitch-style rollout flag: flip on to allow reading v2, flip back to fall in behind
# v1. Enabling it is not sufficient on its own — a request must also be fully inside the
# window v2 has data for, see _use_v2_storage.
CO_OCCURRING_ATTRS_V2_OPTION = "use_co_occurring_attrs_v2"

# Unix timestamp of the first `date` bucket the v2 tables hold data for. The v2 tables and
# their materialized view were created on 2026-07-29, and the view only appends buckets
# from when it started running, so v2 has nothing before the Monday of that week
# (2026-07-27 00:00 UTC). `date` is bucketed weekly with toMonday(), and the query rounds
# its lower bound down to the previous Monday, so the cutoff has to be a Monday too:
# anything later would make a request starting mid-week round below the cutoff and read
# a bucket that only exists in v1.
#
# A request reaching back before this reads v1 instead, otherwise the attributes that only
# existed in the earlier part of its range would silently disappear from the results. Once
# the oldest v2 bucket is older than the longest retention window this becomes dead weight
# and can be dropped along with the v1 read path.
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION = "co_occurring_attrs_v2_start_timestamp"
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT = 1785110400  # 2026-07-27 00:00:00 UTC

# (key-array column, AttributeKey type name) per attribute type on the v2 storage, which
# stores one key array per type so each key is surfaced with its own type.
V2_TYPE_KEY_ARRAYS: Mapping[AttributeKey.Type.ValueType, tuple[str, str]] = {
    AttributeKey.Type.TYPE_STRING: ("attributes_string", "TYPE_STRING"),
    # backwards compatibility with TYPE_FLOAT: same column, the requested type name
    AttributeKey.Type.TYPE_FLOAT: ("attributes_float", "TYPE_FLOAT"),
    AttributeKey.Type.TYPE_DOUBLE: ("attributes_float", "TYPE_DOUBLE"),
    AttributeKey.Type.TYPE_INT: ("attributes_int", "TYPE_INT"),
    AttributeKey.Type.TYPE_BOOLEAN: ("attributes_bool", "TYPE_BOOLEAN"),
    AttributeKey.Type.TYPE_ARRAY_STRING: ("attributes_array_string", "TYPE_ARRAY_STRING"),
    AttributeKey.Type.TYPE_ARRAY_INT: ("attributes_array_int", "TYPE_ARRAY_INT"),
    AttributeKey.Type.TYPE_ARRAY_DOUBLE: ("attributes_array_float", "TYPE_ARRAY_DOUBLE"),
    AttributeKey.Type.TYPE_ARRAY_BOOL: ("attributes_array_bool", "TYPE_ARRAY_BOOL"),
}

# The deprecated untyped TYPE_ARRAY has no element type, so it surfaces the keys of all
# four element-typed array maps, each tagged with its own type.
V2_ARRAY_KEY_ARRAYS: list[tuple[str, str]] = [
    V2_TYPE_KEY_ARRAYS[attr_type]
    for attr_type in (
        AttributeKey.Type.TYPE_ARRAY_STRING,
        AttributeKey.Type.TYPE_ARRAY_INT,
        AttributeKey.Type.TYPE_ARRAY_DOUBLE,
        AttributeKey.Type.TYPE_ARRAY_BOOL,
    )
]

# TYPE_UNSPECIFIED surfaces every type. `attributes_int` is deliberately left out: int
# attributes are double-written to a float bucket on eap_items, so they are already in
# `attributes_float` and including both arrays would emit each int key twice (once as
# TYPE_DOUBLE, once as TYPE_INT). An explicit TYPE_INT request reads `attributes_int`.
V2_UNSPECIFIED_KEY_ARRAYS: list[tuple[str, str]] = [
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_STRING],
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_DOUBLE],
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_BOOLEAN],
    *V2_ARRAY_KEY_ARRAYS,
]


def _v2_covers_request_window(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether v2 has data for the whole time range the request asks about.

    The comparison is against the request's *rounded* lower bound rather than the raw
    start timestamp, because that is the bucket the query actually reads: a request
    starting Wednesday reads from the Monday of that week (see
    ``get_co_occurring_attributes_date_condition``). Comparing the raw timestamp would let
    a request starting just after the cutoff read the preceding, non-existent bucket.
    """
    start_timestamp = get_option(
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    )
    earliest_bucket = prev_monday(
        request.meta.start_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)
    )
    # ToDatetime() returns a naive UTC datetime, so drop the tzinfo to compare like-for-like
    v2_start = datetime.fromtimestamp(start_timestamp, UTC).replace(tzinfo=None)
    return earliest_bucket >= v2_start


def _use_v2_storage(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether to read the v2 co-occurring-attributes storage for this request.

    Requires both the rollout flag and that v2 actually has data covering the requested
    range; a request reaching further back transparently falls back to v1, which has the
    full history.
    """
    if not get_option(CO_OCCURRING_ATTRS_V2_OPTION, False):
        return False
    return _v2_covers_request_window(request)


def _order_by_count(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller opted into frequency ordering via ``order_by`` (sort:-count()).

    When ``order_by`` is unset, ``column`` defaults to COLUMN_UNSPECIFIED, so the
    endpoint keeps its historical name-ascending ordering and existing consumers
    are unaffected.
    """
    return request.order_by.column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT


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


def _typed_key_arrays(
    request: TraceItemAttributeNamesRequest, *, use_v2: bool
) -> list[tuple[str, str]]:
    """The (key-array column, ``AttributeKey`` type name) pairs the request's ``type``
    reads on the selected storage.

    v1 only has string/float/bool key arrays: int keys live in the float array (so an int
    request reads it and its keys come back typed TYPE_DOUBLE) and array-typed keys are
    not stored at all (so an array-typed request falls back to all three arrays, as it
    always has). v2 keeps one key array per attribute type, so every key is surfaced with
    its own type and array-typed requests are answered natively.
    """
    if not use_v2:
        if request.type == AttributeKey.Type.TYPE_STRING:
            return [("attributes_string", "TYPE_STRING")]
        if request.type == AttributeKey.Type.TYPE_FLOAT:
            # backwards compatibility with TYPE_FLOAT
            return [("attributes_float", "TYPE_FLOAT")]
        if request.type in (AttributeKey.Type.TYPE_DOUBLE, AttributeKey.Type.TYPE_INT):
            return [("attributes_float", "TYPE_DOUBLE")]
        if request.type == AttributeKey.Type.TYPE_BOOLEAN:
            return [("attributes_bool", "TYPE_BOOLEAN")]
        # TYPE_UNSPECIFIED (and any type v1 cannot answer natively)
        return [
            ("attributes_string", "TYPE_STRING"),
            ("attributes_float", "TYPE_DOUBLE"),
            ("attributes_bool", "TYPE_BOOLEAN"),
        ]

    if request.type == AttributeKey.Type.TYPE_ARRAY:
        # deprecated untyped TYPE_ARRAY: no element type, so surface all four
        return list(V2_ARRAY_KEY_ARRAYS)
    typed = V2_TYPE_KEY_ARRAYS.get(request.type)
    if typed is not None:
        return [typed]
    # TYPE_UNSPECIFIED (or any type with no dedicated column) surfaces every type
    return list(V2_UNSPECIFIED_KEY_ARRAYS)


def _searched_key_array_columns(
    request: TraceItemAttributeNamesRequest, *, use_v2: bool
) -> list[str]:
    """The key-array columns the request's ``type`` reads on the selected storage."""
    return [col for col, _ in _typed_key_arrays(request, use_v2=use_v2)]


def _add_substring_match_optimization(
    request: TraceItemAttributeNamesRequest,
    condition: Expression,
    *,
    use_v2: bool,
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
        f.arrayExists(like_lambda, column(col))
        for col in _searched_key_array_columns(request, use_v2=use_v2)
    ]
    if not exists:
        return condition
    if len(exists) == 1:
        return and_cond(condition, exists[0])
    return and_cond(condition, or_cond(*exists))


def get_co_occurring_attributes(
    request: TraceItemAttributeNamesRequest,
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

          -- Opt-in frequency ordering (order_by.column = COLUMN_COUNT) instead groups
          -- and counts the keys, returning the most common first. On the v1 storage the
          -- frequency is a row count; on v2 (a SummingMergeTree carrying an occurrence
          -- `count` per attribute set) it is the sum of that column, which approximates
          -- the number of items the key was seen on:
          --   SELECT arrayJoin(...) AS attr_key, sum(count) AS count
          --   ... GROUP BY attr_key ORDER BY count DESC, attr_key ASC

      **Storage:** reads `eap_item_co_occurring_attrs` (v1) or, when the
      `use_co_occurring_attrs_v2` option is on *and* the requested time range is fully
      inside the window v2 has data for, `eap_item_co_occurring_attrs_v2`. v2 keeps one
      key array per attribute type, so int and array-typed keys are surfaced with their
      real `AttributeKey` type instead of being folded into the float array (v1 cannot
      answer array-typed requests at all). Requests reaching back before v2 exists fall
      back to v1 rather than returning a partial result.

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
    use_v2 = _use_v2_storage(request)

    # get all attribute keys from the filter
    collector = AttributeKeyCollector()
    TraceItemFilterWrapper(request.intersecting_attributes_filter).accept(collector)
    attribute_keys_to_search = collector.keys
    storage_key = CO_OCCURRING_ATTRS_V2_STORAGE_KEY if use_v2 else CO_OCCURRING_ATTRS_STORAGE_KEY

    storage = Storage(
        key=storage_key,
        schema=get_storage(storage_key).get_schema().get_columns(),
        sample=None,
    )

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
    condition = _add_substring_match_optimization(request, condition, use_v2=use_v2)

    if request.meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
        condition = and_cond(f.equals(column("item_type"), request.meta.trace_item_type), condition)

    # One (type, key) tuple array per key-array column the request reads, so every key
    # carries the AttributeKey type of the column it came from. A single-type request
    # reads one array; TYPE_UNSPECIFIED (and untyped TYPE_ARRAY) concatenates several.
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
        for col, type_name in _typed_key_arrays(request, use_v2=use_v2)
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
    if _order_by_count(request):
        # Opt-in frequency ordering: group by key and count the co-occurring attribute
        # sets containing it. On v1 (one row per distinct attribute set) that is a row
        # count; on v2 each row carries an occurrence `count` that the SummingMergeTree
        # accumulates, so sum it to approximate the number of items with that key.
        count_expression = (
            f.sum(column("count"), alias="count") if use_v2 else f.count(alias="count")
        )
        selected_columns = [
            SelectedExpression(name="attr_key", expression=attr_key_expression),
            SelectedExpression(name="count", expression=count_expression),
        ]
        groupby: list[Expression] | None = [column("attr_key")]
        order_by = [
            OrderBy(
                direction=(
                    OrderByDirection.DESC if request.order_by.descending else OrderByDirection.ASC
                ),
                expression=column("count"),
            ),
            # stable tiebreak for keys with the same frequency (semver key when
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
        from_clause=storage,
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


def convert_co_occurring_results_to_attributes(
    request: TraceItemAttributeNamesRequest,
    query_res: QueryResult,
) -> list[TraceItemAttributeNamesResponse.Attribute]:
    def t(row: Row) -> TraceItemAttributeNamesResponse.Attribute:
        attr_type, attr_name = row["attr_key"]
        assert isinstance(attr_type, str)
        attribute = TraceItemAttributeNamesResponse.Attribute(
            name=attr_name, type=getattr(AttributeKey.Type, attr_type)
        )
        # `count` is only selected when ordering by frequency; surface it for the
        # real attributes. The synthetic non-stored attributes have no count.
        count = row.get("count")
        if count is not None:
            attribute.count = int(count)
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
        if _order_by_count(request):
            # Match ClickHouse: count in the requested direction, then name ASC
            # (two stable passes). Synthetic non-stored keys have no count, so
            # pin them first rather than relying on a sentinel.
            data.sort(key=_name_key)
            data.sort(key=lambda row: row.get("count", 0), reverse=request.order_by.descending)
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
        snuba_request = get_co_occurring_attributes(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )

        response = TraceItemAttributeNamesResponse(
            attributes=convert_co_occurring_results_to_attributes(in_msg, res),
            meta=extract_response_meta(
                in_msg.meta.request_id, in_msg.meta.debug, [res], [self._timer]
            ),
        )
        return response
