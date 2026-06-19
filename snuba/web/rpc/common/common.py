import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar, cast

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AnyAttributeFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba import settings, state
from snuba.clickhouse import DATETIME_FORMAT
from snuba.protos.common import (
    ATTRIBUTES_TO_COALESCE,
    COLUMN_PREFIX,
    PROTO_TYPE_TO_ATTRIBUTE_COLUMN,
    PROTO_TYPE_TO_CLICKHOUSE_TYPE,
    MalformedAttributeException,
    type_array_to_membership_array_expression,
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
    Expression,
    FunctionCall,
    JsonPath,
    Lambda,
    SubscriptableReference,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def inline_in_to_has(expression: Expression) -> Expression:
    """Rewrite ``in(x, array(...))`` as ``has(array(...), x)`` for SELECT-clause use.

    A constant ``IN`` set makes ClickHouse build an internal prepared set whose
    server-generated identifier (``__set_<Type>_<hash>_<hash>``) is baked into the
    result-block column name. When such an expression lands in a SELECT-clause
    expression — a conditional aggregate (``countIf``/``sumIf``/...), a ``HAVING``
    aggregate, or an ``arrayJoin`` projection — that name is matched by string across
    ``Remote`` nodes. On a mixed-version ClickHouse cluster the two sides hash the set
    differently, so the names disagree and the distributed read fails with
    ``Code: 10 ... Not found column ... While executing Remote.`` (SNUBA-9W6,
    SNUBA-B82).

    ``has`` over a constant array keeps the array inline in the column name, which is
    byte-stable across versions, and ``has(array, x)`` is equivalent to ``x IN (array)``
    for scalar membership. The surrounding NULL-handling wrapper built by
    ``trace_item_filters_to_expression`` is preserved untouched.

    Only apply this to expressions destined for a SELECT clause / projection / aggregate
    condition / ``HAVING`` / ``GROUP BY`` / ``ORDER BY`` computed column. Do NOT apply it
    to WHERE-clause conditions: there the prepared ``IN`` set is what lets ClickHouse
    prune partitions and primary-key ranges, so replacing it with ``has`` would force
    full scans.
    """

    def rewrite(exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "in" and len(exp.parameters) == 2:
            lhs, rhs = exp.parameters
            # Only constant arrays (literals_array -> array(...)) build a prepared set.
            if isinstance(rhs, FunctionCall) and rhs.function_name == "array":
                # Build the FunctionCall directly (rather than f.has(..., alias=...)) so the
                # original Optional[str] alias is preserved without a type error.
                return FunctionCall(exp.alias, "has", (rhs, lhs))
        return exp

    return expression.transform(rewrite)


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


def _trace_item_filter_key_expression(
    attr_to_key_expression_callable: Callable[[AttributeKey], Expression], key: AttributeKey
) -> Expression:
    """predicates must use the normalized
    ``arrayMap`` (``type_array_to_membership_array_expression``) so
    ``arrayExists`` compares per element. It is different from SELECT predicate.
    """
    if key.type == AttributeKey.Type.TYPE_ARRAY:
        try:
            return type_array_to_membership_array_expression(key)
        except MalformedAttributeException as e:
            raise BadSnubaRPCRequestException(str(e)) from e
    return attr_to_key_expression_callable(key)


Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)

BUCKET_COUNT = 40


def transform_array_value(value: Any) -> Any:
    """Decode one array element: with a String, Int, Double, or Bool tag."""
    if not isinstance(value, dict):
        raise BadSnubaRPCRequestException(
            f"array element must be an object with a String/Int/Double/Bool tag, got {type(value).__name__}"
        )
    for t, v in value.items():
        if t == "Int":
            return int(v)
        if t == "Double":
            return float(v)
        if t == "Bool":
            return str(v).lower() == "true"
        if t == "String":
            return str(v)
    raise BadSnubaRPCRequestException(
        f"array value has no recognized tag, keys={list(value.keys())}"
    )


# Allowlist of `attributes_array` JSON sub-paths exposed by endpoints that
# return all attributes (TraceItemDetails, GetTrace bulk-fetch). Each path is
# read as its own JSON sub-column so we don't materialize the full dynamic
# JSON value on every request.
ATTRIBUTES_ARRAY_ALLOWLIST: tuple[str, ...] = (
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.request.messages",
    "gen_ai.response.text",
    "gen_ai.system_instructions",
    "gen_ai.tool.definitions",
    "gen_ai.response.object",
    "gen_ai.tool.call.arguments",
    "gen_ai.tool.input",
    "workflow_ids",
    "triggered_workflow_ids",
    "action_filter_group_ids",
    "triggered_action_ids",
)


def attributes_array_selected_expressions() -> list[SelectedExpression]:
    """Per-path `toJSONString(attributes_array.<path>.:Array(JSON))` selects for the allowlist."""
    return [
        SelectedExpression(
            path,
            FunctionCall(
                alias=path,
                function_name="toJSONString",
                parameters=(
                    JsonPath(
                        alias=None,
                        base=column("attributes_array"),
                        path=path,
                        return_type="Array(JSON)",
                    ),
                ),
            ),
        )
        for path in ATTRIBUTES_ARRAY_ALLOWLIST
    ]


def decode_attributes_array_value(key: str, raw: Any) -> list[Any] | str | None:
    """Decode a `toJSONString(...:Array(JSON))` payload for an allowlisted path.

    Returns None if `raw` is not a string (caller should skip). If `key` is
    in `ATTRIBUTES_ARRAY_ALLOWLIST` and `raw` looks like a JSON array (starts
    with '['), parse it and normalize each element via
    `transform_array_value`. Malformed JSON or non-tagged elements fall back
    to the raw string. Otherwise return `raw` unchanged — either the JSON
    path resolved to a non-array or `key` isn't an attributes_array path at
    all. Callers should still skip empty list results.
    """
    if not isinstance(raw, str):
        return None
    if key not in ATTRIBUTES_ARRAY_ALLOWLIST or not raw.startswith("["):
        return raw
    try:
        return [transform_array_value(elem) for elem in json.loads(raw)]
    except (json.JSONDecodeError, BadSnubaRPCRequestException):
        return raw


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
    use_sampling_factor_timestamp_seconds = cast(
        int,
        state.get_int_config(
            "use_sampling_factor_timestamp_seconds",
            settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS,
        ),
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
        elif exp.function_name == "or":
            return combine_or_conditions(exp.parameters)
        else:
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
}


def _typed_null_for_map_column(column_name: str) -> Expression:
    for prefix, null_type in _MAP_COLUMN_NULL_TYPE.items():
        if column_name.startswith(prefix):
            return f.cast(literal(None), null_type)
    return literal(None)


def add_existence_check_to_subscriptable_references(query: Query) -> None:
    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, SubscriptableReference):
            return exp

        return FunctionCall(
            alias=exp.alias,
            function_name="if",
            parameters=(
                map_key_exists(exp.column, exp.key),
                SubscriptableReference(None, exp.column, exp.key),
                _typed_null_for_map_column(exp.column.column_name),
            ),
        )

    query.transform_expressions(transform)


def _contains_subscriptable_reference(exp: Expression) -> bool:
    """True if ``exp`` contains a ``SubscriptableReference`` (a map lookup) — the
    map-backed keys we route through ``_map_backed_operands``."""
    found = False

    def visit(node: Expression) -> Expression:
        nonlocal found
        if isinstance(node, SubscriptableReference):
            found = True
        return node

    exp.transform(visit)
    return found


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
    empty value, since ``arrayElement`` reads both as the '' / 0 default. The
    ``multiIf`` else is only reached when all keys are absent.

    Built without aliases: conditions don't need them, and an alias here would
    collide with the SELECT clause's existence ``if(...)`` for the same attribute
    (same alias, different expression). Only valid for map-backed keys
    (string/int/float); booleans / normalized columns / arrays take the legacy
    path, and SELECT keeps its own ``coalesce(...)`` representation untouched.
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
        for cond, val in zip(existences[:-1], values[:-1]):
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
) -> Expression:
    """``IN`` / ``NOT IN`` as ``[not] in(value, set)``, wrapped in
    ``and(exists, ...)`` only when ``guard`` is set (see ``_map_backed_operands``
    and ``_comparison_can_match_column_default``). The value lists carry only
    scalars, so the set never contains NULL — the legacy ``has(set, NULL)`` branch
    was always ``false``."""
    value, exists = _map_backed_operands(k)
    if ignore_case:
        value = f.lower(value)
    membership = in_cond(value, v_expression)
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


def _comparison_can_match_column_default(
    attr_type: AttributeKey.Type.ValueType, v: AttributeValue, value_type: str
) -> bool:
    """True if any compared literal is the column default ('' / 0), which an
    absent key also reads as — so the existence guard is needed to avoid
    matching absent keys. When no literal is the default the guard is dropped (the
    simplest form). LIKE/NOT_LIKE always guard; null comparisons are separate."""
    default: str | int = "" if attr_type == AttributeKey.Type.TYPE_STRING else 0
    if value_type == "val_array":
        scalars: list[Any] = [_scalar_value(x) for x in v.val_array.values]
    elif value_type in ("val_str_array", "val_int_array", "val_float_array", "val_double_array"):
        scalars = list(getattr(v, value_type).values)
    else:
        scalars = [_scalar_value(v)]
    return any(s == default for s in scalars)


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
    op: ComparisonFilter.Op.ValueType, v: AttributeValue
) -> None:
    if op in (ComparisonFilter.OP_LIKE, ComparisonFilter.OP_NOT_LIKE):
        if v.WhichOneof("value") != "val_str":
            raise BadSnubaRPCRequestException(
                "LIKE/NOT_LIKE on array keys requires a string pattern"
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


def _type_array_membership_rhs_expression(v: AttributeValue) -> Expression:
    """RHS as String, comparable to TYPE_ARRAY arrayMap output (Array(String) in CH)."""
    value_type = v.WhichOneof("value")
    match value_type:
        case "val_str":
            return literal(v.val_str)
        case "val_int":
            return f.toString(literal(v.val_int))
        case "val_double":
            return f.toString(literal(v.val_double))
        case "val_float":
            return f.toString(literal(v.val_float))
        case "val_bool":
            return literal(str(v.val_bool).lower())
        case _:
            raise BadSnubaRPCRequestException(
                f"unsupported AttributeValue for array membership: {value_type}"
            )


def _type_array_includes_scalar_expression(
    array_expr: Expression,
    v: AttributeValue,
    ignore_case: bool,
) -> Expression:
    """Any element equals scalar (includes / [*])"""
    if v.WhichOneof("value") == "val_null" or v.is_null:
        raise BadSnubaRPCRequestException("Arrays can't be NULL or cannot have NULL elements")
    x = Argument(None, "x")
    rhs = _type_array_membership_rhs_expression(v)
    if ignore_case and v.WhichOneof("value") == "val_str":
        return f.arrayExists(
            Lambda(None, ("x",), f.equals(f.lower(x), f.lower(rhs))),
            array_expr,
        )
    return f.arrayExists(Lambda(None, ("x",), f.equals(x, rhs)), array_expr)


def _any_attribute_filter_to_expression(
    filt: AnyAttributeFilter,
) -> Expression:
    """Build an expression that searches across attribute values.

    The column to search is derived from the value type to avoid
    ClickHouse type mismatches (e.g. comparing a string against a Float64 column).

    Generates::

        arrayExists(x -> <comparison>(x, value), mapValues(column))

    wrapped with NOT(...) for negative ops.
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
            comparison = in_cond(
                f.lower(x),
                literals_array(None, lowered),
            )
        else:
            comparison = in_cond(x, v_expression)
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


def trace_item_filters_to_expression(
    item_filter: TraceItemFilter,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
    """
    Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
    This maps those filters into an expression which can be used in a WHERE clause
    :param item_filter:
    :return:
    """
    if item_filter.HasField("and_filter"):
        filters = item_filter.and_filter.filters
        if len(filters) == 0:
            return literal(True)
        elif len(filters) == 1:
            return trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
        return and_cond(
            *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
        )

    if item_filter.HasField("or_filter"):
        filters = item_filter.or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'or' clause")
        elif len(filters) == 1:
            return trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
        return or_cond(
            *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
        )

    if item_filter.HasField("not_filter"):
        filters = item_filter.not_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'not' clause")
        elif len(filters) == 1:
            return not_cond(
                trace_item_filters_to_expression(filters[0], attribute_key_to_expression)
            )
        return not_cond(
            and_cond(
                *(trace_item_filters_to_expression(x, attribute_key_to_expression) for x in filters)
            )
        )

    if item_filter.HasField("comparison_filter"):
        k = item_filter.comparison_filter.key
        op = item_filter.comparison_filter.op
        v = item_filter.comparison_filter.value

        if k.type == AttributeKey.Type.TYPE_ARRAY:
            _validate_comparison_filter_type_array(op, v)

        k_expression = _trace_item_filter_key_expression(
            attr_to_key_expression_callable=attribute_key_to_expression, key=k
        )

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException("comparison does not have a right hand side")

        v_is_null = v.is_null or value_type == "val_null"
        if v_is_null:
            v_expression: Expression = literal(None)
        else:
            v_expression = _attribute_value_to_expression(v)

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

            if k.type == AttributeKey.Type.TYPE_ARRAY:
                return _type_array_includes_scalar_expression(
                    k_expression, v, item_filter.comparison_filter.ignore_case
                )
            if _contains_subscriptable_reference(k_expression):
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
                if _comparison_can_match_column_default(k.type, v, value_type):
                    return and_cond(exists, cmp)
                return cmp
            else:
                expr = (
                    f.equals(f.lower(k_expression), f.lower(v_expression))
                    if item_filter.comparison_filter.ignore_case
                    else f.equals(k_expression, v_expression)
                )
                # we redefine the way equals works for nulls
                # now null=null is true
                expr_with_null = or_cond(
                    expr, and_cond(f.isNull(k_expression), f.isNull(v_expression))
                )
                return expr_with_null
        if op == ComparisonFilter.OP_NOT_EQUALS:
            _check_non_string_values_cannot_ignore_case(item_filter.comparison_filter)
            if k.type == AttributeKey.Type.TYPE_ARRAY:
                return not_cond(
                    _type_array_includes_scalar_expression(
                        k_expression, v, item_filter.comparison_filter.ignore_case
                    )
                )
            if _contains_subscriptable_reference(k_expression):
                # Negation of OP_EQUALS; an absent key is "not equal".
                value, exists = _map_backed_operands(k)
                if v_is_null:  # `attr != null` <=> key present
                    return exists
                lhs, rhs = (
                    (f.lower(value), f.lower(v_expression))
                    if item_filter.comparison_filter.ignore_case
                    else (value, v_expression)
                )
                if _comparison_can_match_column_default(k.type, v, value_type):
                    return not_cond(and_cond(exists, f.equals(lhs, rhs)))
                return f.notEquals(lhs, rhs)
            else:
                expr = (
                    f.notEquals(f.lower(k_expression), f.lower(v_expression))
                    if item_filter.comparison_filter.ignore_case
                    else f.notEquals(k_expression, v_expression)
                )
                # we redefine the way not equals works for nulls
                # now null!=null is true
                expr_with_null = or_cond(
                    expr, f.xor(f.isNull(k_expression), f.isNull(v_expression))
                )
                return expr_with_null
        if op == ComparisonFilter.OP_LIKE:
            if k.type == AttributeKey.Type.TYPE_ARRAY:
                like_fn = f.ilike if item_filter.comparison_filter.ignore_case else f.like
                return f.arrayExists(
                    Lambda(
                        None,
                        ("x",),
                        like_fn(Argument(None, "x"), v_expression),
                    ),
                    k_expression,
                )
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the LIKE comparison is only supported on string and array keys"
                )
            comparison_function = f.ilike if item_filter.comparison_filter.ignore_case else f.like
            if _contains_subscriptable_reference(k_expression):
                value, exists = _map_backed_operands(k)
                return and_cond(exists, comparison_function(value, v_expression))
            return comparison_function(k_expression, v_expression)
        if op == ComparisonFilter.OP_NOT_LIKE:
            if k.type == AttributeKey.Type.TYPE_ARRAY:
                like_fn = f.ilike if item_filter.comparison_filter.ignore_case else f.like
                return not_cond(
                    f.arrayExists(
                        Lambda(
                            None,
                            ("x",),
                            like_fn(Argument(None, "x"), v_expression),
                        ),
                        k_expression,
                    )
                )
            if k.type != AttributeKey.Type.TYPE_STRING:
                raise BadSnubaRPCRequestException(
                    "the NOT LIKE comparison is only supported on string and array keys"
                )
            if _contains_subscriptable_reference(k_expression):
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
            if _contains_subscriptable_reference(k_expression):
                # Map-backed: keep the existence if(...) out of in() (see helper).
                return _analyzer_safe_in_expression(
                    k,
                    v_expression,
                    negated=False,
                    ignore_case=ignore_case,
                    guard=_comparison_can_match_column_default(k.type, v, value_type),
                )
            if ignore_case:
                k_expression = f.lower(k_expression)
            expr = in_cond(k_expression, v_expression)
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
            if _contains_subscriptable_reference(k_expression):
                # Map-backed: keep the existence if(...) out of in() (see helper).
                return _analyzer_safe_in_expression(
                    k,
                    v_expression,
                    negated=True,
                    ignore_case=ignore_case,
                    guard=_comparison_can_match_column_default(k.type, v, value_type),
                )
            if ignore_case:
                k_expression = f.lower(k_expression)
            expr = not_cond(in_cond(k_expression, v_expression))
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
        if not state.get_int_config("enable_any_attribute_filter", 1):
            return literal(True)
        return _any_attribute_filter_to_expression(item_filter.any_attribute_filter)

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
    return f.toDateTime(
        datetime.fromtimestamp(ts_seconds, tz=timezone.utc).strftime(DATETIME_FORMAT)
    )


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
        elif isinstance(field, FunctionCall) and len(field.parameters) > 0:
            if len(field.parameters) > 0 and isinstance(
                field.parameters[0], SubscriptableReference
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
        return map_key_exists(field.parameters[0], field.parameters[1])

    if isinstance(field, FunctionCall) and field.function_name == "arrayMap":
        # Array attributes in the JSON column return empty arrays (not NULL)
        # for missing keys, so notEmpty is the correct existence check.
        return f.notEmpty(field)

    return f.isNotNull(field)
