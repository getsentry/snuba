import math
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, cast

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AnyAttributeFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import settings
from snuba.clickhouse import DATETIME_FORMAT
from snuba.protos.common import (
    ARRAY_TYPES,
    ATTRIBUTES_TO_COALESCE,
    COLUMN_PREFIX,
    NORMALIZED_COLUMNS_EAP_ITEMS,
    PROTO_TYPE_TO_ATTRIBUTE_COLUMN,
    PROTO_TYPE_TO_CLICKHOUSE_TYPE,
    TYPED_ARRAY_MAP_COLUMNS,
    MalformedAttributeException,
    array_element_column,
    type_array_to_membership_array_expression_from_typed_columns,
    type_array_typed_column_native_array,
)
from snuba.protos.common import (
    attribute_key_to_expression as _attribute_key_to_expression,
)
from snuba.query import Query, SelectedExpression
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    arrayElement,
    column,
    in_cond,
    literal,
    literals_array,
    map_key_exists,
    not_cond,
    or_cond,
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


def _trace_item_filter_key_expression(
    attr_to_key_expression_callable: Callable[[AttributeKey], Expression],
    key: AttributeKey,
) -> Expression:
    """Array predicates read a per-element array so ``arrayExists`` can compare each
    element (different from the SELECT expression). An element-typed array key reads its
    single typed column natively; the deprecated untyped ``TYPE_ARRAY`` concatenates all
    four typed columns (normalized to ``Array(String)``). Distinct alias from the SELECT
    expression avoids a SELECT/WHERE alias collision for the same attribute.
    """
    if key.type in ARRAY_TYPES:
        try:
            col = array_element_column(key)
            if col is not None:
                return type_array_typed_column_native_array(key, col)
            return type_array_to_membership_array_expression_from_typed_columns(key)
        except MalformedAttributeException as e:
            raise BadSnubaRPCRequestException(str(e)) from e
    return attr_to_key_expression_callable(key)


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


def _check_non_string_values_cannot_ignore_case(
    comparison_filter: ComparisonFilter,
) -> None:
    if not comparison_filter.ignore_case:
        return
    value_type = comparison_filter.value.WhichOneof("value")
    if value_type == "val_array":
        if not all(
            elem.WhichOneof("value") == "val_str"
            for elem in comparison_filter.value.val_array.values
        ):
            raise BadSnubaRPCRequestException("Cannot ignore case on non-string values")
    elif value_type not in ("val_str", "val_str_array"):
        raise BadSnubaRPCRequestException("Cannot ignore case on non-string values")


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


def _is_map_backed_key(k: AttributeKey) -> bool:
    """True when ``k`` is a custom attribute stored in an ``attributes_*`` map column, so
    its filters take the ``(value, exists)`` path (see ``_map_backed_operands``). Excludes
    ``attr_key`` and normalized columns, which are real columns, not map lookups."""
    return (
        k.name != "attr_key"
        and k.name not in NORMALIZED_COLUMNS_EAP_ITEMS
        and k.type in PROTO_TYPE_TO_ATTRIBUTE_COLUMN
    )


def _map_backed_operands(k: AttributeKey) -> tuple[Expression, Expression]:
    """Build ``(value, exists)`` for a map-backed key directly, NULL-free.

    The legacy ``if(<exists>, arrayElement, NULL)`` form puts a NULL constant
    into the predicate; the new ClickHouse analyzer canonicalizes that NULL
    inconsistently between an aggregate's projection name and its computed block
    (notably inside ``in(...)``), so the column isn't found ("Code: 10 ... Not
    found column ... in block"). We write a ``has(mapKeys(...))`` existence check
    (see ``map_key_exists``) + ``arrayElement`` directly, so no NULL appears:

        value  = arrayElement(k)                                       # single key
        value  = multiIf(has(mapKeys(k1)), arrayElement(k1), ...,      # coalesced:
                         arrayElement(kn))                             # first present
        exists = has(mapKeys(k1)) OR ... OR has(mapKeys(kn))

    Callers build ``cmp(value, v)``, wrapped in ``and(exists, ...)`` only when the
    literal could be the column default (see ``_comparison_can_match_column_default``)
    — then ``exists``, not the value, distinguishes a missing key from a stored
    empty value, since ``arrayElement`` reads both as the '' / 0 / false default. The
    ``multiIf`` else is only reached when all keys are absent.

    Built without aliases: conditions don't need them, and an alias here would
    collide with the SELECT clause's existence ``if(...)`` for the same attribute
    (same alias, different expression). Only valid for map-backed scalar keys
    (string/int/float/bool, see ``_is_map_backed_key``); normalized columns and arrays
    are excluded (arrays take their own element-wise path), and SELECT keeps its own
    ``coalesce(...)`` representation untouched.
    """
    col_name = PROTO_TYPE_TO_ATTRIBUTE_COLUMN[k.type]
    names = [k.name] + list(ATTRIBUTES_TO_COALESCE.get(k.name, ()))

    def _value(name: str) -> Expression:
        elem = arrayElement(None, column(col_name), literal(name))
        # ints live in the float map and surface as Int64, so they need a cast.
        if k.type == AttributeKey.Type.TYPE_INT:
            return f.cast(elem, f"Nullable({PROTO_TYPE_TO_CLICKHOUSE_TYPE[k.type]})")
        return elem

    values = [_value(name) for name in names]
    existences = [map_key_exists(column(col_name), literal(name)) for name in names]

    exists = combine_or_conditions(existences) if len(existences) > 1 else existences[0]
    if len(values) == 1:
        value: Expression = values[0]
    else:
        args: list[Expression] = []
        for cond, val in zip(existences[:-1], values[:-1], strict=True):
            args.extend((cond, val))
        args.append(values[-1])
        value = f.multiIf(*args)
    return value, exists


def _analyzer_safe_in_expression(
    k: AttributeKey,
    v_expression: Expression,
    *,
    negated: bool,
    ignore_case: bool = False,
    guard: bool = True,
    membership_as_has: bool = False,
) -> Expression:
    """``IN`` / ``NOT IN`` as ``[not] in(value, set)``, wrapped in
    ``and(exists, ...)`` only when ``guard`` is set (see ``_map_backed_operands``
    and ``_comparison_can_match_column_default``). The value lists carry only
    scalars, so the set never contains NULL — the legacy ``has(set, NULL)`` branch
    was always ``false``. ``membership_as_has`` emits ``has(set, value)`` instead of
    ``in(value, set)`` for SELECT-clause use (see ``_in_or_has``)."""
    value, exists = _map_backed_operands(k)
    if ignore_case:
        value = f.lower(value)
    membership = _in_or_has(value, v_expression, as_has=membership_as_has)
    present = and_cond(exists, membership) if guard else membership
    return not_cond(present) if negated else present


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


def _comparison_can_match_column_default(v: AttributeValue, value_type: str) -> bool:
    """True if any compared literal is the column default — its type's falsy value
    ('' / 0 / false), which an absent key also reads as — so the existence guard is needed
    to avoid matching absent keys. When no literal is the default the guard is dropped (the
    simplest form). LIKE/NOT_LIKE always guard; null comparisons are separate."""
    if value_type == "val_array":
        scalars: list[Any] = [_scalar_value(x) for x in v.val_array.values]
    elif value_type in ("val_str_array", "val_int_array", "val_float_array", "val_double_array"):
        scalars = list(getattr(v, value_type).values)
    else:
        scalars = [_scalar_value(v)]
    return any(not s for s in scalars if s is not None)


def _attribute_value_to_expression(v: AttributeValue) -> Expression:
    """Convert an AttributeValue proto to a Snuba Expression."""
    value_type = v.WhichOneof("value")
    match value_type:
        case "val_bool":
            return literal(v.val_bool)
        case "val_str":
            return literal(v.val_str)
        case "val_float":
            return literal(v.val_float)
        case "val_double":
            return literal(v.val_double)
        case "val_int":
            return literal(v.val_int)
        case "val_array":
            return literals_array(None, [literal(_scalar_value(x)) for x in v.val_array.values])
        case "val_str_array" | "val_int_array" | "val_float_array" | "val_double_array":
            return literals_array(None, [literal(x) for x in getattr(v, value_type).values])
        case default:
            raise NotImplementedError(
                f"translation of AttributeValue type {default} is not implemented"
            )


_NEGATIVE_OPS = {
    AnyAttributeFilter.OP_NOT_EQUALS,
    AnyAttributeFilter.OP_NOT_LIKE,
    AnyAttributeFilter.OP_NOT_IN,
}

_POSITIVE_OP_FOR_NEGATIVE: dict[int, int] = {
    AnyAttributeFilter.OP_NOT_EQUALS: AnyAttributeFilter.OP_EQUALS,
    AnyAttributeFilter.OP_NOT_LIKE: AnyAttributeFilter.OP_LIKE,
    AnyAttributeFilter.OP_NOT_IN: AnyAttributeFilter.OP_IN,
}

_STRING_COLUMNS = {"attributes_string"}

# Map scalar value types to the ClickHouse column they're compatible with
_VALUE_TYPE_TO_COLUMN: dict[str, str] = {
    "val_str": "attributes_string",
    "val_int": "attributes_float",
    "val_float": "attributes_float",
    "val_double": "attributes_float",
    "val_bool": "attributes_bool",
    # Deprecated per-type array fields (still supported)
    "val_str_array": "attributes_string",
    "val_int_array": "attributes_float",
    "val_float_array": "attributes_float",
    "val_double_array": "attributes_float",
}

_ARRAY_VALUE_TYPES = {
    "val_array",
    "val_str_array",
    "val_int_array",
    "val_float_array",
    "val_double_array",
}


def _validate_comparison_filter_type_array(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    if op in (ComparisonFilter.OP_LIKE, ComparisonFilter.OP_NOT_LIKE):
        if v.WhichOneof("value") != "val_str":
            raise BadSnubaRPCRequestException(
                "LIKE/NOT_LIKE on array keys requires a string pattern"
            )
        # LIKE only matches string elements, so it makes sense only for string arrays.
        if key.type not in (AttributeKey.Type.TYPE_ARRAY, AttributeKey.Type.TYPE_ARRAY_STRING):
            raise BadSnubaRPCRequestException(
                "LIKE/NOT_LIKE on array keys is only supported on string arrays "
                f"(TYPE_ARRAY_STRING), got {AttributeKey.Type.Name(key.type)}"
            )
        return
    if op in (ComparisonFilter.OP_EQUALS, ComparisonFilter.OP_NOT_EQUALS):
        # Array can be empty or non-empty. It can never be null, or can never have null elements.
        vt = v.WhichOneof("value")
        if vt in (
            None,
            "val_null",
            "val_array",
            "val_str_array",
            "val_int_array",
            "val_float_array",
            "val_double_array",
        ):
            raise BadSnubaRPCRequestException(
                "OP_EQUALS/OP_NOT_EQUALS on array keys require a scalar value "
                "(e.g. val_str, val_int) or null (is_null / val_null) to match null elements"
            )
        return
    raise BadSnubaRPCRequestException(
        f"{ComparisonFilter.Op.Name(op)} is not supported on array keys "
        "(supported: LIKE, NOT_LIKE, OP_EQUALS, OP_NOT_EQUALS)"
    )


def _coerce_int(s: str) -> int | None:
    try:
        return int(s)
    except ValueError:
        return None


def _coerce_float(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None


def _native_literal_for_array_column(col: str, v: AttributeValue) -> Expression:
    """The filter value coerced to a single typed array column's native element type.

    An element-typed array key names its column exactly, so we coerce ``v`` to that
    element type (accepting the natively-typed value or a ``val_str`` that parses to it —
    Sentry historically sends array-membership values as ``val_str``) and raise if it
    can't match the column at all."""
    value_type = v.WhichOneof("value")
    if col == "attributes_array_string":
        if value_type == "val_str":
            return literal(v.val_str)
        raise BadSnubaRPCRequestException("string array comparison requires a string value")
    if col == "attributes_array_int":
        if value_type == "val_int":
            return literal(v.val_int)
        if value_type == "val_str" and (iv := _coerce_int(v.val_str)) is not None:
            return literal(iv)
        raise BadSnubaRPCRequestException("int array comparison requires an integer value")
    if col == "attributes_array_float":
        if value_type in ("val_double", "val_float"):
            return literal(getattr(v, value_type))
        if value_type == "val_int":
            return literal(float(v.val_int))
        if value_type == "val_str" and (fv := _coerce_float(v.val_str)) is not None:
            return literal(fv)
        raise BadSnubaRPCRequestException("double array comparison requires a numeric value")
    if col == "attributes_array_bool":
        if value_type == "val_bool":
            return literal(v.val_bool)
        if value_type == "val_str" and v.val_str.lower() in ("true", "false"):
            return literal(v.val_str.lower() == "true")
        raise BadSnubaRPCRequestException("bool array comparison requires a boolean value")
    raise BadSnubaRPCRequestException(f"unknown array column: {col}")


def _typed_array_native_membership_candidates(
    attr_key: AttributeKey,
    v: AttributeValue,
) -> list[tuple[str, Expression]]:
    """``(typed column, native rhs)`` pairs for an array-membership comparison on the
    typed ``attributes_array_*`` columns.

    An element-typed array key (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL) resolves to exactly
    one column, so a single candidate is returned with the value coerced to that column's
    native type — no cross-column OR. The deprecated untyped ``TYPE_ARRAY`` has no element
    type, so a ``val_str`` is coerced to every native type it parses as and each matching
    column is searched (OR-ed by the caller).
    """
    col = array_element_column(attr_key)
    if col is not None:
        return [(col, _native_literal_for_array_column(col, v))]

    value_type = v.WhichOneof("value")
    candidates: list[tuple[str, Expression]] = []
    if value_type == "val_str":
        s = v.val_str
        candidates.append(("attributes_array_string", literal(s)))
        int_val = _coerce_int(s)
        if int_val is not None:
            candidates.append(("attributes_array_int", literal(int_val)))
        float_val = _coerce_float(s)
        if float_val is not None:
            candidates.append(("attributes_array_float", literal(float_val)))
        if s.lower() in ("true", "false"):
            candidates.append(("attributes_array_bool", literal(s.lower() == "true")))
    elif value_type == "val_int":
        candidates.append(("attributes_array_int", literal(v.val_int)))
        candidates.append(("attributes_array_float", literal(float(v.val_int))))
    elif value_type in ("val_float", "val_double"):
        candidates.append(("attributes_array_float", literal(getattr(v, value_type))))
    elif value_type == "val_bool":
        candidates.append(("attributes_array_bool", literal(v.val_bool)))
    else:
        raise BadSnubaRPCRequestException(
            f"unsupported AttributeValue for array membership: {value_type}"
        )
    return candidates


def _typed_array_includes_scalar_expression(
    attr_key: AttributeKey,
    v: AttributeValue,
    ignore_case: bool,
) -> Expression:
    """Any element equals scalar (includes / [*]) against the typed ``attributes_array_*``
    columns: a native ``arrayExists`` per candidate column, OR-ed together (see
    ``_typed_array_native_membership_candidates``)."""
    if v.WhichOneof("value") == "val_null" or v.is_null:
        raise BadSnubaRPCRequestException("Arrays can't be NULL or cannot have NULL elements")
    exprs: list[Expression] = []
    for col, rhs in _typed_array_native_membership_candidates(attr_key, v):
        array_expr = type_array_typed_column_native_array(attr_key, col)
        x = Argument(None, "x")
        if ignore_case and col == "attributes_array_string":
            lam = Lambda(None, ("x",), f.equals(f.lower(x), f.lower(rhs)))
        else:
            lam = Lambda(None, ("x",), f.equals(x, rhs))
        exprs.append(f.arrayExists(lam, array_expr))
    if len(exprs) == 1:
        return exprs[0]
    return or_cond(exprs[0], exprs[1], *exprs[2:])


def _typed_array_like_expression(
    attr_key: AttributeKey, pattern: Expression, ignore_case: bool
) -> Expression:
    """LIKE membership against the typed columns. A pattern can only match string
    elements, so read just ``attributes_array_string``."""
    array_expr = type_array_typed_column_native_array(attr_key, "attributes_array_string")
    like_fn = f.ilike if ignore_case else f.like
    return f.arrayExists(
        Lambda(None, ("x",), like_fn(Argument(None, "x"), pattern)),
        array_expr,
    )


def _any_attribute_filter_to_expression(
    filt: AnyAttributeFilter,
    *,
    membership_as_has: bool = False,
) -> Expression:
    """Build an expression that searches across attribute values.

    The column to search is derived from the value type to avoid
    ClickHouse type mismatches (e.g. comparing a string against a Float64 column).

    Generates::

        arrayExists(x -> <comparison>(x, value), mapValues(column))

    wrapped with NOT(...) for negative ops. ``membership_as_has`` builds the IN/NOT_IN
    comparison as ``has(array, x)`` rather than ``in(x, array)`` so the (arrayExists'd)
    expression carries no ``__set_*`` prepared-set identifier — required when it lands
    in a SELECT-clause aggregate on a mixed-version distributed read (see ``_in_or_has``).
    """
    # 1. Extract and validate the comparison value
    v = filt.value
    value_type = v.WhichOneof("value")
    if value_type is None or value_type == "val_null":
        raise BadSnubaRPCRequestException("any_attribute_filter does not have a value")

    # Resolve the effective op for building the lambda (negation handled at the end)
    is_negative = filt.op in _NEGATIVE_OPS
    effective_op = _POSITIVE_OP_FOR_NEGATIVE.get(filt.op, filt.op)

    is_array = value_type in _ARRAY_VALUE_TYPES
    if effective_op == AnyAttributeFilter.OP_IN and not is_array:
        raise BadSnubaRPCRequestException(
            "IN/NOT_IN operations require an array value type (val_array)"
        )

    if effective_op != AnyAttributeFilter.OP_IN and is_array:
        raise BadSnubaRPCRequestException(
            f"{AnyAttributeFilter.Op.Name(filt.op)} does not support array values, use OP_IN/OP_NOT_IN"
        )

    # Validate that IN/NOT_IN arrays are non-empty
    if effective_op == AnyAttributeFilter.OP_IN:
        arr_values = (
            v.val_array.values if value_type == "val_array" else getattr(v, value_type).values
        )
        if len(arr_values) == 0:
            raise BadSnubaRPCRequestException("IN/NOT_IN operations require a non-empty array")

    # 2. Determine which column to search based on the value type
    if value_type == "val_array":
        elem_types = {elem.WhichOneof("value") for elem in v.val_array.values}
        if len(elem_types) != 1:
            raise BadSnubaRPCRequestException("val_array elements must all be the same type")
        elem_type = elem_types.pop()
        if elem_type not in _VALUE_TYPE_TO_COLUMN:
            raise BadSnubaRPCRequestException(f"Unsupported array element type: {elem_type}")
        col_name = _VALUE_TYPE_TO_COLUMN[elem_type]
    else:
        if value_type not in _VALUE_TYPE_TO_COLUMN:
            raise BadSnubaRPCRequestException(
                f"Unsupported value type for any_attribute_filter: {value_type}"
            )
        col_name = _VALUE_TYPE_TO_COLUMN[value_type]

    # LIKE/NOT_LIKE only makes sense on string columns
    if effective_op == AnyAttributeFilter.OP_LIKE and col_name not in _STRING_COLUMNS:
        raise BadSnubaRPCRequestException(
            "LIKE/NOT_LIKE operations are only supported on string values"
        )

    # ignore_case uses lower() which only works on string columns
    if filt.ignore_case and col_name not in _STRING_COLUMNS:
        raise BadSnubaRPCRequestException("Cannot ignore case on non-string values")

    v_expression = _attribute_value_to_expression(v)

    # 3. Build the lambda comparison
    x = Argument(None, "x")

    if effective_op == AnyAttributeFilter.OP_EQUALS:
        if filt.ignore_case:
            comparison = f.equals(f.lower(x), f.lower(v_expression))
        else:
            comparison = f.equals(x, v_expression)
    elif effective_op == AnyAttributeFilter.OP_LIKE:
        if filt.ignore_case:
            comparison = f.ilike(x, v_expression)
        else:
            comparison = f.like(x, v_expression)
    elif effective_op == AnyAttributeFilter.OP_IN:
        if filt.ignore_case:
            if value_type == "val_str_array":
                lowered = [literal(s.lower()) for s in v.val_str_array.values]
            else:
                lowered = [literal(elem.val_str.lower()) for elem in v.val_array.values]
            comparison = _in_or_has(
                f.lower(x),
                literals_array(None, lowered),
                as_has=membership_as_has,
            )
        else:
            comparison = _in_or_has(x, v_expression, as_has=membership_as_has)
    else:
        raise BadSnubaRPCRequestException(
            f"Unsupported any_attribute_filter op: {AnyAttributeFilter.Op.Name(filt.op)}"
        )

    lam = Lambda(None, ("x",), comparison)

    # 4. Build the arrayExists expression for the single matching column.
    positive_expr = f.arrayExists(lam, f.mapValues(column(col_name)))

    if is_negative:
        return not_cond(positive_expr)
    return positive_expr


# Per-item-type primary name promoted to the indexed_name column on ingestion
# (rust_snuba/src/processors/eap_items.rs, migration 0057).
_INDEXED_NAME_KEY_BY_ITEM_TYPE: dict[int, str] = {
    TraceItemType.TRACE_ITEM_TYPE_SPAN: "sentry.op",
    TraceItemType.TRACE_ITEM_TYPE_METRIC: "sentry.metric.name",
}

# Organization_ids the indexed_name redirect is enabled for. Only add an org once
# indexed_name is backfilled for its queried retention window.
USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION = "eap_items_use_indexed_name_organization_ids"


def indexed_name_key(organization_id: int, item_type: int) -> str | None:
    """The attribute whose eap_items filter should read the indexed ``indexed_name``
    column, or ``None`` to keep the unindexed ``attributes_string`` bucket lookup."""
    org_ids = cast("list[int]", get_option(USE_INDEXED_NAME_ORGANIZATION_IDS_OPTION, []))
    if organization_id not in org_ids:
        return None
    return _INDEXED_NAME_KEY_BY_ITEM_TYPE.get(item_type)


def indexed_name_key_for_request(meta: RequestMeta) -> str | None:
    return indexed_name_key(meta.organization_id, meta.trace_item_type)


def trace_item_filters_to_expression(
    item_filter: TraceItemFilter,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    membership_as_has: bool = False,
    indexed_name_key: str | None = None,
) -> Expression:
    """
    Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
    This maps those filters into an expression which can be used in a WHERE clause
    :param item_filter:
    :param membership_as_has: build ``IN``/``NOT IN`` membership as ``has(array, x)``
        rather than ``x IN (array)``. Pass ``True`` only when the result lands in a
        SELECT clause / projection / aggregate condition / ``HAVING`` — there a constant
        ``IN`` set leaks an unstable ``__set_*`` identifier into the result-block column
        name and breaks mixed-version distributed reads (see ``_in_or_has``). Leave the
        default for WHERE clauses, where the prepared ``IN`` set drives pruning.
    :return:

    Array predicates always read the typed ``attributes_array_*`` map columns: an
    element-typed array key (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL) hits its single column
    natively, the deprecated untyped ``TYPE_ARRAY`` searches all four.
    """
    if item_filter.HasField("and_filter"):
        filters = item_filter.and_filter.filters
        if len(filters) == 0:
            return literal(True)
        if len(filters) == 1:
            return trace_item_filters_to_expression(
                filters[0],
                attribute_key_to_expression,
                membership_as_has,
                indexed_name_key,
            )
        return and_cond(
            *(
                trace_item_filters_to_expression(
                    x,
                    attribute_key_to_expression,
                    membership_as_has,
                    indexed_name_key,
                )
                for x in filters
            )
        )

    if item_filter.HasField("or_filter"):
        filters = item_filter.or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'or' clause")
        if len(filters) == 1:
            return trace_item_filters_to_expression(
                filters[0],
                attribute_key_to_expression,
                membership_as_has,
                indexed_name_key,
            )
        return or_cond(
            *(
                trace_item_filters_to_expression(
                    x,
                    attribute_key_to_expression,
                    membership_as_has,
                    indexed_name_key,
                )
                for x in filters
            )
        )

    if item_filter.HasField("not_filter"):
        filters = item_filter.not_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'not' clause")
        if len(filters) == 1:
            return not_cond(
                trace_item_filters_to_expression(
                    filters[0],
                    attribute_key_to_expression,
                    membership_as_has,
                    indexed_name_key,
                )
            )
        return not_cond(
            and_cond(
                *(
                    trace_item_filters_to_expression(
                        x,
                        attribute_key_to_expression,
                        membership_as_has,
                        indexed_name_key,
                    )
                    for x in filters
                )
            )
        )

    if item_filter.HasField("comparison_filter"):
        k = item_filter.comparison_filter.key
        op = item_filter.comparison_filter.op
        v = item_filter.comparison_filter.value

        if k.type in ARRAY_TYPES:
            _validate_comparison_filter_type_array(op, v, k)

        k_expression = _trace_item_filter_key_expression(
            attr_to_key_expression_callable=attribute_key_to_expression,
            key=k,
        )

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException("comparison does not have a right hand side")

        v_is_null = v.is_null or value_type == "val_null"
        if v_is_null:
            v_expression: Expression = literal(None)
        else:
            v_expression = _attribute_value_to_expression(v)

        # Redirect the promoted name filter to the bloom-filter-indexed indexed_name
        # column (cf. the sentry.timestamp special-case below) so it's granule-prunable.
        # Only for a plain non-null, non-default value: indexed_name can't tell an absent
        # key from an empty one, so those cases keep the exists-guarded bucket lookup.
        redirect_to_indexed_name = (
            indexed_name_key is not None
            and k.name == indexed_name_key
            and k.type == AttributeKey.Type.TYPE_STRING
            and not v_is_null
            and not _comparison_can_match_column_default(v, value_type)
        )
        if redirect_to_indexed_name:
            k_expression = column("indexed_name")
        map_backed = _is_map_backed_key(k) and not redirect_to_indexed_name

        # `sentry.timestamp` is a normalized column that `attribute_key_to_expression`
        # maps to `CAST(timestamp, 'Float64')`. Wrapping the primary-key/partition
        # column in a CAST prevents ClickHouse from using it for granule and partition
        # pruning, so range filters on it scan far more data than necessary. It also
        # duplicates the mandatory time-range condition (timestamp_in_range_condition)
        # that is already applied on the raw column. For range comparisons, compare
        # against the raw DateTime `timestamp` column instead so the condition is
        # index- and partition-prunable. We reuse timestamp_seconds_to_datetime_literal
        # so a bound equal to the mandatory range is byte-identical to it and gets
        # collapsed by dedupe_timestamp_conditions.
        if k.name == f"{COLUMN_PREFIX}timestamp" and value_type in (
            "val_int",
            "val_float",
            "val_double",
        ):
            scalar_value = _scalar_value(v)
            assert isinstance(scalar_value, (int, float))
            raw_timestamp = column("timestamp")
            # `timestamp` is a second-resolution DateTime, so a fractional bound must be
            # rounded to the integer second that preserves the original
            # `CAST(timestamp, 'Float64') OP value` result: `<`/`>=` round up (ceil) and
            # `<=`/`>` round down (floor). Integer bounds are left unchanged, so the
            # rewritten mandatory-range bounds stay byte-identical and
            # dedupe_timestamp_conditions can still collapse them.
            if op == ComparisonFilter.OP_LESS_THAN:
                return f.less(
                    raw_timestamp,
                    timestamp_seconds_to_datetime_literal(math.ceil(scalar_value)),
                )
            if op == ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
                return f.lessOrEquals(
                    raw_timestamp,
                    timestamp_seconds_to_datetime_literal(math.floor(scalar_value)),
                )
            if op == ComparisonFilter.OP_GREATER_THAN:
                return f.greater(
                    raw_timestamp,
                    timestamp_seconds_to_datetime_literal(math.floor(scalar_value)),
                )
            if op == ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
                return f.greaterOrEquals(
                    raw_timestamp,
                    timestamp_seconds_to_datetime_literal(math.ceil(scalar_value)),
                )

        if op == ComparisonFilter.OP_EQUALS:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)

            if k.type in ARRAY_TYPES:
                return _typed_array_includes_scalar_expression(
                    k, v, item_filter.comparison_filter.ignore_case
                )
            if map_backed:
                # Map-backed: NULL-free (exists, value) form (see _map_backed_operands).
                value, exists = _map_backed_operands(k)
                if v_is_null:  # `attr = null` <=> key absent
                    return not_cond(exists)
                lhs, rhs = (
                    (f.lower(value), f.lower(v_expression))
                    if item_filter.comparison_filter.ignore_case
                    else (value, v_expression)
                )
                cmp = f.equals(lhs, rhs)
                # existence guard only needed when '' / 0 could match an absent key.
                if _comparison_can_match_column_default(v, value_type):
                    return and_cond(exists, cmp)
                return cmp
            expr = (
                f.equals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.equals(k_expression, v_expression)
            )
            # we redefine the way equals works for nulls
            # now null=null is true
            expr_with_null = or_cond(expr, and_cond(f.isNull(k_expression), f.isNull(v_expression)))
            return expr_with_null
        if op == ComparisonFilter.OP_NOT_EQUALS:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            if k.type in ARRAY_TYPES:
                return not_cond(
                    _typed_array_includes_scalar_expression(
                        k, v, item_filter.comparison_filter.ignore_case
                    )
                )
            if map_backed:
                # Negation of OP_EQUALS; an absent key is "not equal".
                value, exists = _map_backed_operands(k)
                if v_is_null:  # `attr != null` <=> key present
                    return exists
                lhs, rhs = (
                    (f.lower(value), f.lower(v_expression))
                    if item_filter.comparison_filter.ignore_case
                    else (value, v_expression)
                )
                if _comparison_can_match_column_default(v, value_type):
                    return not_cond(and_cond(exists, f.equals(lhs, rhs)))
                return f.notEquals(lhs, rhs)
            expr = (
                f.notEquals(f.lower(k_expression), f.lower(v_expression))
                if item_filter.comparison_filter.ignore_case
                else f.notEquals(k_expression, v_expression)
            )
            # we redefine the way not equals works for nulls
            # now null!=null is true
            expr_with_null = or_cond(expr, f.xor(f.isNull(k_expression), f.isNull(v_expression)))
            return expr_with_null
        if op == ComparisonFilter.OP_LIKE:
            if k.type in ARRAY_TYPES:
                return _typed_array_like_expression(
                    k, v_expression, item_filter.comparison_filter.ignore_case
                )
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the LIKE comparison is only supported on string and array keys"
                )
            comparison_function = f.ilike if item_filter.comparison_filter.ignore_case else f.like
            if map_backed:
                value, exists = _map_backed_operands(k)
                return and_cond(exists, comparison_function(value, v_expression))
            return comparison_function(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_LIKE:
            if k.type in ARRAY_TYPES:
                return not_cond(
                    _typed_array_like_expression(
                        k, v_expression, item_filter.comparison_filter.ignore_case
                    )
                )
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the NOT LIKE comparison is only supported on string and array keys"
                )
            if map_backed:
                # Negation of OP_LIKE; an absent key is "not like".
                like_fn = f.ilike if item_filter.comparison_filter.ignore_case else f.like
                value, exists = _map_backed_operands(k)
                return not_cond(and_cond(exists, like_fn(value, v_expression)))
            comparison_function = (
                f.notILike if item_filter.comparison_filter.ignore_case else f.notLike
            )
            expr = comparison_function(k_expression, v_expression)
            # we redefine the way not like works for nulls
            # now null not like "%anything%" is true
            expr_with_null = or_cond(expr, f.isNull(k_expression))
            return expr_with_null
        if op == ComparisonFilter.OP_LESS_THAN:
            return f.less(k_expression, v_expression)
        if op == ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
            return f.lessOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN:
            return f.greater(k_expression, v_expression)
        if op == ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
            return f.greaterOrEquals(k_expression, v_expression)
        if op == ComparisonFilter.OP_IN:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            ignore_case = item_filter.comparison_filter.ignore_case
            if ignore_case:
                if value_type == "val_str_array":
                    v_expression = literals_array(
                        None,
                        [literal(s.lower()) for s in v.val_str_array.values],
                    )
                else:
                    v_expression = literals_array(
                        None,
                        [literal(elem.val_str.lower()) for elem in v.val_array.values],
                    )
            # note: v_expression must be an array
            # we redefine the way in works for nulls
            # now null in ['hi', null] is true
            if map_backed:
                # Map-backed: keep the existence if(...) out of in() (see helper).
                return _analyzer_safe_in_expression(
                    k,
                    v_expression,
                    negated=False,
                    ignore_case=ignore_case,
                    guard=_comparison_can_match_column_default(v, value_type),
                    membership_as_has=membership_as_has,
                )
            if ignore_case:
                k_expression = f.lower(k_expression)
            expr = _in_or_has(k_expression, v_expression, as_has=membership_as_has)
            expr_with_null = or_cond(
                expr,
                and_cond(f.isNull(k_expression), f.has(v_expression, literal(None))),
            )
            return expr_with_null
        if op == ComparisonFilter.OP_NOT_IN:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            ignore_case = item_filter.comparison_filter.ignore_case
            if ignore_case:
                if value_type == "val_str_array":
                    v_expression = literals_array(
                        None,
                        [literal(s.lower()) for s in v.val_str_array.values],
                    )
                else:
                    v_expression = literals_array(
                        None,
                        [literal(elem.val_str.lower()) for elem in v.val_array.values],
                    )
            # note: v_expression must be an array
            # we redefine the way not in works for nulls
            # now null not in ['hi'] is true
            if map_backed:
                # Map-backed: keep the existence if(...) out of in() (see helper).
                return _analyzer_safe_in_expression(
                    k,
                    v_expression,
                    negated=True,
                    ignore_case=ignore_case,
                    guard=_comparison_can_match_column_default(v, value_type),
                    membership_as_has=membership_as_has,
                )
            if ignore_case:
                k_expression = f.lower(k_expression)
            expr = not_cond(_in_or_has(k_expression, v_expression, as_has=membership_as_has))
            expr_with_null = or_cond(
                expr,
                and_cond(
                    f.isNull(k_expression),
                    not_cond(f.has(v_expression, literal(None))),
                ),
            )
            return expr_with_null

        raise BadSnubaRPCRequestException(
            f"Invalid string comparison, unknown op: {item_filter.comparison_filter}"
        )

    if item_filter.HasField("exists_filter"):
        return get_field_existence_expression(
            _trace_item_filter_key_expression(
                attr_to_key_expression_callable=attribute_key_to_expression,
                key=item_filter.exists_filter.key,
            )
        )

    if item_filter.HasField("any_attribute_filter"):
        if not get_option("enable_any_attribute_filter", True):
            return literal(True)
        return _any_attribute_filter_to_expression(
            item_filter.any_attribute_filter, membership_as_has=membership_as_has
        )

    return literal(True)


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

    return f.isNotNull(field)
