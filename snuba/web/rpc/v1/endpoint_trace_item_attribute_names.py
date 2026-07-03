import re
import uuid

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


def _order_by_count(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller opted into frequency ordering via ``order_by`` (sort:-count()).

    When ``order_by`` is unset, ``column`` defaults to COLUMN_UNSPECIFIED, so the
    endpoint keeps its historical name-ascending ordering and existing consumers
    are unaffected.
    """
    return request.order_by.column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT


def _order_by_name_descending(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller explicitly requested name ordering in descending order.

    Only ``COLUMN_NAME`` + ``descending`` flips the default; unset ordering stays
    name-ascending for backwards compatibility.
    """
    return (
        request.order_by.column == TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_NAME
        and request.order_by.descending
    )


def _order_by_natural(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether the caller requested SORT_NATURAL (semver) ordering of names."""
    return request.order_by.sort == TraceItemAttributeNamesRequest.OrderBy.SORT_NATURAL


_SEMVER_NUMERIC_RE = re.compile(r"^[0-9]+(\.[0-9]+)*$")


def _semver_sort_key_py(name: str) -> tuple[tuple[int, int, int, int], int, str]:
    """Python mirror of common.semver_sort_key, used to re-sort names in Python so
    the merged (ClickHouse + synthetic) result matches the ClickHouse ORDER BY."""
    non_null = name or ""
    version_no_prefix = non_null.split("@")[-1]
    release_part = version_no_prefix.split("-")[0]
    components = [int(c) if c.isdigit() else 0 for c in release_part.split(".")]
    components = (components + [0, 0, 0, 0])[:4]
    is_stable = 1 if _SEMVER_NUMERIC_RE.match(version_no_prefix) else 0
    return (
        (components[0], components[1], components[2], components[3]),
        is_stable,
        non_null,
    )


def _name_order_by_expression(natural: bool) -> Expression:
    """ClickHouse ORDER BY expression for the attribute name.

    Default orders by the raw (type, name) tuple; SORT_NATURAL orders by the
    semver key of the name part so versions sort numerically.
    """
    if natural:
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

    if request.type == AttributeKey.Type.TYPE_STRING:
        return and_cond(condition, f.arrayExists(like_lambda, column("attributes_string")))
    if request.type in (
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_DOUBLE,
        AttributeKey.Type.TYPE_INT,
    ):
        return and_cond(condition, f.arrayExists(like_lambda, column("attributes_float")))
    if request.type == AttributeKey.Type.TYPE_BOOLEAN:
        return and_cond(condition, f.arrayExists(like_lambda, column("attributes_bool")))
    # TYPE_UNSPECIFIED - check all arrays with OR
    return and_cond(
        condition,
        or_cond(
            f.arrayExists(like_lambda, column("attributes_string")),
            f.arrayExists(like_lambda, column("attributes_float")),
            f.arrayExists(like_lambda, column("attributes_bool")),
        ),
    )


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
          -- and counts the keys, returning the most common first:
          --   SELECT arrayJoin(...) AS attr_key, count() AS count
          --   ... GROUP BY attr_key ORDER BY count DESC, attr_key ASC

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
    # get all attribute keys from the filter
    collector = AttributeKeyCollector()
    TraceItemFilterWrapper(request.intersecting_attributes_filter).accept(collector)
    attribute_keys_to_search = collector.keys
    storage_key = StorageKey("eap_item_co_occurring_attrs")

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
    condition = _add_substring_match_optimization(request, condition)

    if request.meta.trace_item_type != TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED:
        condition = and_cond(f.equals(column("item_type"), request.meta.trace_item_type), condition)

    string_array = f.arrayMap(
        Lambda(
            None,
            ("x",),
            f.tuple(
                "TYPE_STRING",
                column("x"),
            ),
        ),
        column("attributes_string"),
    )

    # backwards compatibility with TYPE_FLOAT
    floating_point_type = (
        "TYPE_FLOAT" if request.type == AttributeKey.Type.TYPE_FLOAT else "TYPE_DOUBLE"
    )

    double_array = f.arrayMap(
        Lambda(
            None,
            ("x",),
            f.tuple(
                floating_point_type,
                column("x"),
            ),
        ),
        column("attributes_float"),
    )

    bool_array = f.arrayMap(
        Lambda(
            None,
            ("x",),
            f.tuple(
                "TYPE_BOOLEAN",
                column("x"),
            ),
        ),
        column("attributes_bool"),
    )

    array_func = None
    if request.type == AttributeKey.Type.TYPE_STRING:
        array_func = string_array
    elif request.type in (
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_DOUBLE,
        AttributeKey.Type.TYPE_INT,
    ):
        array_func = double_array
    elif request.type == AttributeKey.Type.TYPE_BOOLEAN:
        array_func = bool_array
    else:
        array_func = f.arrayConcat(string_array, double_array, bool_array)

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

    natural = _order_by_natural(request)
    if _order_by_count(request):
        # Opt-in frequency ordering: group by key and count how many rows
        # (co-occurring attribute sets) contain each key.
        selected_columns = [
            SelectedExpression(name="attr_key", expression=attr_key_expression),
            SelectedExpression(name="count", expression=f.count(alias="count")),
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
            # SORT_NATURAL was requested)
            OrderBy(direction=OrderByDirection.ASC, expression=_name_order_by_expression(natural)),
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
                expression=_name_order_by_expression(natural),
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
    # tuple by default, or the semver key of the name under SORT_NATURAL.
    natural = _order_by_natural(request)

    def _name_key(row: dict) -> object:
        attr_key = row.get("attr_key", ("TYPE_STRING", ""))
        attr_type, attr_name = attr_key[0], attr_key[1]
        if natural:
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
            # Order the real (counted) rows to match ClickHouse: count in the
            # requested direction, then name ASC (two stable passes). The synthetic
            # non-stored attributes have no real count, so pin them first regardless
            # of sort direction rather than relying on a sentinel value.
            data.sort(key=_name_key)
            data.sort(key=lambda row: row.get("count", 0), reverse=request.order_by.descending)
            data = non_stored + data
        else:
            # Default name ordering: merge the synthetic non-stored keys in and re-sort
            # by name, honoring the requested direction so it matches the ClickHouse
            # ORDER BY (a COLUMN_NAME descending request must stay descending here too).
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
