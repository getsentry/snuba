from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.protos.common import (
    EMPTY_STRING_DEFAULT_COLUMNS,
    PROTO_TYPE_TO_ATTRIBUTE_COLUMN,
    TYPED_ARRAY_MAP_COLUMNS,
    MalformedAttributeException,
)
from snuba.protos.common import (
    attribute_key_to_expression as _attribute_key_to_expression,
)
from snuba.query import Query, SelectedExpression
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    column,
    in_cond,
    literal,
    literals_array,
    map_key_exists,
)
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    SubscriptableReference,
)
from snuba.state.sentry_options import get_option
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def _in_or_has(value: Expression, array: Expression, *, as_has: bool) -> FunctionCall:
    """Build the membership test ``value IN array``.

    Returns ``in(value, array)`` by default, so ClickHouse keeps a prepared set for
    partition/primary-key pruning — correct for WHERE clauses. When ``as_has`` is set,
    returns ``has(array, value)`` instead, for expressions that land in a SELECT clause
    / projection / aggregate condition / ``HAVING``. There the prepared set's
    server-generated ``__set_<Type>_<hash>_<hash>`` identifier leaks into the
    result-block column name and is not byte-stable across mixed-version distributed
    ClickHouse nodes, which fails the read with
    ``Code: 10 ... Not found column ... While executing Remote.`` (SNUBA-9W6, SNUBA-B82).
    ``has`` over a constant array keeps the array inline in the column name and is
    equivalent to ``value IN (array)`` for scalar membership.
    """
    if as_has:
        return f.has(array, value)
    return in_cond(value, array)


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    """Convert an AttributeKey proto to a Snuba Expression.

    This is a wrapper around the proto-layer function that converts
    MalformedAttributeException to BadSnubaRPCRequestException for
    HTTP-aware code paths.

    Raises:
        BadSnubaRPCRequestException: If the attribute key is invalid or malformed.
    """
    try:
        return _attribute_key_to_expression(attr_key)
    except MalformedAttributeException as e:
        raise BadSnubaRPCRequestException(str(e)) from e


_SEMVER_COMPONENT_COUNT = 4  # major.minor.patch.build


def semver_sort_key(expr: Expression, alias: str | None = None) -> Expression:
    """Return a Tuple(Array(UInt32), UInt8, String) semver sort key for ``SORT_SEMVER``.

    Callers opt in per request (no hardcoded attribute list) and the key is
    applied to whatever column they order by. Strips a 'package@' prefix and
    '+build' metadata, maps the release part to 4 UInt32 components (so "1.2" ==
    "1.2.0"), then adds a stability flag (0=prerelease, 1=stable) so prereleases
    sort before their stable release, and the raw string as a tiebreaker for a
    deterministic total order. Works on Altinity 25.3/25.8 (no naturalSortKey).
    """
    x = Argument(None, "x")
    # sentry.release is coalesced, so Nullable(String); strip the nullable
    # wrapper (ClickHouse forbids Nullable(Array(…))) before string→array funcs.
    non_null = f.ifNull(expr, literal(""))
    version_no_prefix = f.arrayElement(f.splitByChar(literal("@"), non_null), literal(-1))
    # Drop build metadata (does not affect precedence); left attached it would
    # zero the last component and fail the stability match.
    version_no_build = f.arrayElement(f.splitByChar(literal("+"), version_no_prefix), literal(1))
    release_part = f.arrayElement(f.splitByChar(literal("-"), version_no_build), literal(1))
    numeric_key = f.arrayResize(
        f.arrayMap(
            Lambda(None, ("x",), f.toUInt32OrZero(x)),
            f.splitByChar(literal("."), release_part),
        ),
        literal(_SEMVER_COMPONENT_COUNT),
    )
    # Stable only for plain dotted-numeric versions; anything else (SemVer
    # "-beta.1" or PEP 440 dot-dev "24.7.0.dev0+<sha>") is a prerelease.
    is_stable = f.match(version_no_build, literal(r"^[0-9]+(\.[0-9]+)*$"))
    return FunctionCall(alias, "tuple", (numeric_key, is_stable, non_null))


Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)

BUCKET_COUNT = 40


def typed_array_map_selected_expressions() -> list[SelectedExpression]:
    """Select the four typed array map columns whole, for endpoints that return every
    attribute of an item (TraceItemDetails, GetTrace, ExportTraceItems) without knowing
    the attribute keys or their element types up front, so all array attributes are
    returned."""
    return [SelectedExpression(col, column(col, alias=col)) for col in TYPED_ARRAY_MAP_COLUMNS]


def merge_typed_array_maps(row: dict[str, Any]) -> list[tuple[str, list[Any]]]:
    """Pop the four typed array map columns from ``row`` and merge them into a list of
    ``(attribute_name, elements)`` pairs.

    Each map is ``{name: [native elements of one type]}``. An array attribute whose
    elements span several types appears in multiple maps; its elements are concatenated
    in column order (string, int, float, bool) — the typed columns store each element
    type separately, so cross-type element order is not preserved (homogeneous arrays,
    the common case, keep their order). Names are returned in first-seen order. Callers
    convert each ``elements`` list to a ``val_array`` and skip empty ones."""
    merged: dict[str, list[Any]] = {}
    order: list[str] = []
    for col in TYPED_ARRAY_MAP_COLUMNS:
        column_map = row.pop(col, None) or {}
        for name, values in column_map.items():
            if name not in merged:
                merged[name] = []
                order.append(name)
            merged[name].extend(values)
    return [(name, merged[name]) for name in order]


def typed_array_select_subcolumn_name(base: str, typed_col: str) -> str:
    """Result-column name ``"<base>.<typed_col>"`` for one typed sub-column of a
    per-attribute array SELECT (``base`` is the column label or attribute name)."""
    return f"{base}.{typed_col}"


def merge_typed_array_subcolumns(
    row: dict[str, Any], bases: Iterable[str]
) -> list[tuple[str, list[Any]]]:
    """Pop the four typed sub-columns of each ``base`` array attribute and merge them into
    ``(base, elements)`` pairs (per-attribute counterpart of ``merge_typed_array_maps``).
    Arrays are homogeneous, so one sub-column is non-empty; the four are concatenated in
    column order."""
    merged: list[tuple[str, list[Any]]] = []
    for base in bases:
        elements: list[Any] = []
        for typed_col in TYPED_ARRAY_MAP_COLUMNS:
            values = row.pop(typed_array_select_subcolumn_name(base, typed_col), None)
            if values:
                elements.extend(values)
        merged.append((base, elements))
    return merged


def as_datetime(value: Any) -> datetime:
    """Coerce a ClickHouse DateTime result into a datetime.

    Which the reader returns depends on the driver (native gives a datetime, HTTP can give an
    ISO string), so accept both.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def next_monday(dt: datetime) -> datetime:
    return dt + timedelta(days=(7 - dt.weekday()) or 7)


def prev_monday(dt: datetime) -> datetime:
    return dt - timedelta(days=(dt.weekday() % 7))


def truncate_request_meta_to_day(meta: RequestMeta) -> None:
    # some tables store timestamp as toStartOfDay(x) in UTC, so if you request 4PM - 8PM on a specific day, nada
    # this changes a request from 4PM - 8PM to a request from midnight today to 8PM tomorrow UTC.
    # it also changes 11PM - 1AM to midnight today to 1AM overmorrow
    start_timestamp = datetime.utcfromtimestamp(meta.start_timestamp.seconds)
    end_timestamp = datetime.utcfromtimestamp(meta.end_timestamp.seconds)
    start_timestamp = start_timestamp.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_timestamp = end_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        days=1
    )

    meta.start_timestamp.seconds = int(start_timestamp.timestamp())
    meta.end_timestamp.seconds = int(end_timestamp.timestamp())


def use_sampling_factor(meta: RequestMeta) -> bool:
    """
    Since we started writing the sampling factor on a specific date, we should only use it on queries that start after that date.
    """
    use_sampling_factor_timestamp_seconds = get_option(
        "use_sampling_factor_timestamp_seconds",
        settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS,
    )
    if use_sampling_factor_timestamp_seconds == 0:
        return False

    return meta.start_timestamp.seconds >= use_sampling_factor_timestamp_seconds


def treeify_or_and_conditions(query: Query) -> None:
    """
    look for expressions like or(a, b, c) and turn them into or(a, or(b, c))
                              and(a, b, c) and turn them into and(a, and(b, c))

    even though clickhouse sql supports arbitrary amount of arguments there are other parts of the
    codebase which assume `or` and `and` have two arguments

    Adding this post-process step is easier than changing the rest of the query pipeline

    Note: does not apply to the conditions of a from_clause subquery (the nested one)
        this is bc transform_expressions is not implemented for composite queries

    This also removes structurally-identical conjuncts from the top-level WHERE AND
    (`A AND A` == `A`); see dedupe_and_conditions. Nothing downstream in the query
    pipeline dedupes conditions, so without this the duplicate would reach ClickHouse.
    """
    dedupe_and_conditions(query)

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, FunctionCall):
            return exp

        if exp.function_name == "and":
            return combine_and_conditions(exp.parameters)
        if exp.function_name == "or":
            return combine_or_conditions(exp.parameters)
        return exp

    query.transform_expressions(transform)


def dedupe_and_conditions(query: Query) -> None:
    """Remove structurally-identical conjuncts from the query's top-level AND.

    ``A AND A`` is equivalent to ``A``, so dropping exact duplicates never changes
    results. The motivating case: when a client sends a ``sentry.timestamp`` range
    filter whose bounds equal the mandatory time-range condition, both now produce
    byte-identical expressions (see timestamp_seconds_to_datetime_literal) and
    collapse to a single condition. Distinct ranges are left untouched (the AND of
    them naturally yields the tightest window).

    Conditions nested inside OR/NOT subtrees are treated as opaque leaves -- we only
    flatten the top-level AND. The query is left unchanged unless a duplicate is
    actually found, so existing queries keep their exact condition tree.
    """
    condition = query.get_condition()
    if condition is None:
        return

    def flatten_and(exp: Expression) -> list[Expression]:
        if isinstance(exp, FunctionCall) and exp.function_name == "and":
            flattened: list[Expression] = []
            for param in exp.parameters:
                flattened.extend(flatten_and(param))
            return flattened
        return [exp]

    conjuncts = flatten_and(condition)
    deduped: list[Expression] = []
    for conjunct in conjuncts:
        if conjunct not in deduped:
            deduped.append(conjunct)

    if len(deduped) != len(conjuncts):
        query.set_ast_condition(combine_and_conditions(deduped))


# Map column -> the Nullable type its values resolve to, used to emit a typed
# NULL as the `if(...)` else-branch below. A bare, untyped NULL default trips
# the ClickHouse "new analyzer": it canonicalizes the very same `if(...)`
# expression inconsistently when it appears both as an IN operand and elsewhere
# (e.g. the NOT_IN predicate in `_get_field_existence_expression`), wrapping the
# NULL in a redundant CAST in one place but not the other. The aggregate column
# computed in the block then no longer matches the projection name, surfacing as
# "Code: 10. DB::Exception: Not found column ... in block". Emitting an
# already-typed NULL leaves nothing for the analyzer to fold inconsistently.
_MAP_COLUMN_NULL_TYPE = {
    "attributes_string": "Nullable(String)",
    "attributes_float": "Nullable(Float64)",
    "attributes_bool": "Nullable(Boolean)",
}


def _typed_null_for_map_column(column_name: str) -> Expression:
    for prefix, null_type in _MAP_COLUMN_NULL_TYPE.items():
        if column_name.startswith(prefix):
            return f.cast(literal(None), null_type)
    return literal(None)


_NON_BUCKETED_SCALAR_ATTRIBUTE_MAP_COLUMNS: frozenset[str] = frozenset(
    {PROTO_TYPE_TO_ATTRIBUTE_COLUMN[AttributeKey.Type.TYPE_BOOLEAN]}
)


def _non_bucketed_scalar_map_read(exp: Expression) -> tuple[Column, Expression] | None:
    if not (isinstance(exp, FunctionCall) and exp.function_name == "cast" and exp.parameters):
        return None
    inner = exp.parameters[0]
    if not (
        isinstance(inner, FunctionCall)
        and inner.function_name == "arrayElement"
        and len(inner.parameters) == 2
    ):
        return None
    array_arg, key_arg = inner.parameters
    if (
        isinstance(array_arg, Column)
        and array_arg.column_name in _NON_BUCKETED_SCALAR_ATTRIBUTE_MAP_COLUMNS
    ):
        return array_arg, key_arg
    return None


def add_existence_check_to_map_attribute_reads(query: Query) -> None:
    def transform(exp: Expression) -> Expression:
        if isinstance(exp, SubscriptableReference):
            return FunctionCall(
                alias=exp.alias,
                function_name="if",
                parameters=(
                    map_key_exists(exp.column, exp.key),
                    SubscriptableReference(None, exp.column, exp.key),
                    _typed_null_for_map_column(exp.column.column_name),
                ),
            )

        non_bucketed_read = _non_bucketed_scalar_map_read(exp)
        if non_bucketed_read is not None:
            map_column, key = non_bucketed_read
            return FunctionCall(
                alias=exp.alias,
                function_name="if",
                parameters=(
                    map_key_exists(map_column, key),
                    replace(exp, alias=None),
                    _typed_null_for_map_column(map_column.column_name),
                ),
            )

        return exp

    query.transform_expressions(transform)


def _scalar_value(v: AttributeValue) -> bool | str | int | float | None:
    """Extract a Python scalar from an AttributeValue proto."""
    match v.WhichOneof("value"):
        case "val_bool":
            return v.val_bool
        case "val_str":
            return v.val_str
        case "val_float":
            return v.val_float
        case "val_double":
            return v.val_double
        case "val_int":
            return v.val_int
        case "val_null" | None:
            return None
        case other:
            raise NotImplementedError(f"not a scalar AttributeValue type: {other}")


def project_id_and_org_conditions(meta: RequestMeta) -> Expression:
    return and_cond(
        in_cond(
            column("project_id"),
            literals_array(
                alias=None,
                literals=[literal(pid) for pid in meta.project_ids],
            ),
        ),
        f.equals(column("organization_id"), meta.organization_id),
    )


def timestamp_seconds_to_datetime_literal(ts_seconds: int) -> Expression:
    """Build the canonical ``toDateTime('YYYY-MM-DD HH:MM:SS')`` expression for a unix
    timestamp. The mandatory time-range bounds and rewritten ``sentry.timestamp`` range
    filters share this form so equal bounds produce structurally identical expressions
    (which lets dedupe_timestamp_conditions collapse the duplicates)."""
    return f.toDateTime(datetime.fromtimestamp(ts_seconds, tz=UTC).strftime(DATETIME_FORMAT))


def timestamp_in_range_condition(start_ts: int, end_ts: int) -> Expression:
    return and_cond(
        f.less(column("timestamp"), timestamp_seconds_to_datetime_literal(end_ts)),
        f.greaterOrEquals(column("timestamp"), timestamp_seconds_to_datetime_literal(start_ts)),
    )


def valid_sampling_factor_conditions() -> Expression:
    return and_cond(
        f.lessOrEquals(column("sampling_factor"), 1), f.greater(column("sampling_factor"), 0)
    )


def base_conditions_and(meta: RequestMeta, *other_exprs: Expression) -> Expression:
    """

    :param meta: The RequestMeta field, common across all RPCs
    :param other_exprs: other expressions to add to the *and* clause
    :return: an expression which looks like (project_id IN (a, b, c) AND organization_id=d AND ...)
    """
    return and_cond(
        project_id_and_org_conditions(meta),
        timestamp_in_range_condition(meta.start_timestamp.seconds, meta.end_timestamp.seconds),
        *other_exprs,
    )


def convert_filter_offset(filter_offset: TraceItemFilter) -> Expression:
    if not filter_offset.HasField("comparison_filter"):
        raise TypeError("filter_offset needs to be a comparison filter")
    if filter_offset.comparison_filter.op != ComparisonFilter.OP_GREATER_THAN:
        raise TypeError("filter_offset must use the greater than comparison")

    k_expression = column(filter_offset.comparison_filter.key.name)
    v = filter_offset.comparison_filter.value
    value_type = v.WhichOneof("value")
    if value_type != "val_str":
        raise BadSnubaRPCRequestException("please provide a string for filter offset")

    return f.greater(k_expression, literal(v.val_str))


def get_field_existence_expression(field: Expression) -> Expression:
    def get_subscriptable_field(field: Expression) -> SubscriptableReference | None:
        """
        Check if the field is a subscriptable reference or a function call with a subscriptable reference as the first parameter to handle the case
        where the field is casting a subscriptable reference (e.g. for integers). If so, return the subscriptable reference.
        """
        if isinstance(field, SubscriptableReference):
            return field
        if (
            isinstance(field, FunctionCall)
            and len(field.parameters) > 0
            and isinstance(field.parameters[0], SubscriptableReference)
        ):
            return field.parameters[0]

        return None

    if isinstance(field, FunctionCall) and field.function_name == "coalesce":
        return combine_or_conditions(
            [get_field_existence_expression(param) for param in field.parameters]
        )

    if isinstance(field, FunctionCall) and field.function_name == "multiIf":
        # multiIf(exists1, value1, ..., exists_{n-1}, value_{n-1}, value_n).
        # (Value operands are at the odd indices plus the trailing else.)
        value_operands = list(field.parameters[1::2]) + [field.parameters[-1]]
        return combine_or_conditions(
            [get_field_existence_expression(param) for param in value_operands]
        )

    subscriptable_field = get_subscriptable_field(field)
    if subscriptable_field is not None:
        return map_key_exists(subscriptable_field.column, subscriptable_field.key)

    if isinstance(field, FunctionCall) and field.function_name == "arrayElement":
        base = field.parameters[0]
        # A read of an element-typed array column (arrayElement over one of the typed
        # attributes_array_* maps) returns an empty array for a missing key, so notEmpty
        # is the right existence check. Scalar map lookups still use map_key_exists.
        if isinstance(base, Column) and base.column_name in TYPED_ARRAY_MAP_COLUMNS:
            return f.notEmpty(field)

        return map_key_exists(field.parameters[0], field.parameters[1])

    if isinstance(field, FunctionCall) and field.function_name in ("arrayMap", "arrayConcat"):
        # Array attributes return empty arrays (not NULL) for missing keys, so notEmpty
        # is the correct existence check. This covers the deprecated untyped TYPE_ARRAY
        # membership expression (arrayConcat of the per-type map lookups, see
        # type_array_to_membership_array_expression_from_typed_columns).
        return f.notEmpty(field)

    if isinstance(field, FunctionCall) and field.function_name == "cast":
        # A normalized String column with an empty-string default (e.g. ai_conversation_id)
        # is never NULL, so isNotNull would always be true. notEmpty distinguishes an unset
        # (empty) value from a real one.
        base = field.parameters[0]
        if isinstance(base, Column) and base.column_name in EMPTY_STRING_DEFAULT_COLUMNS:
            return f.notEmpty(field)

    return f.isNotNull(field)
