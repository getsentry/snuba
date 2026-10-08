import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypedDict

from google.protobuf.timestamp_pb2 import Timestamp as ProtobufTimestamp
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    AnyAttributeFilter,
    ComparisonFilter,
    ExistsFilter,
    NotFilter,
    OrFilter,
    TraceItemFilter,
)

from snuba.protos.common import (
    ARRAY_TYPES,
    NORMALIZED_COLUMNS_EAP_ITEMS,
    PROTO_TYPE_TO_ATTRIBUTE_COLUMN,
    PROTO_TYPE_TO_CLICKHOUSE_TYPE,
    MalformedAttributeException,
    array_element_column,
    coalesced_attribute_names,
    first_present_value,
    get_trace_item_type_name,
    key_existence_conditions,
    sentry_column,
    type_array_to_membership_array_expression_from_typed_columns,
    type_array_typed_column_native_array,
)
from snuba.query.conditions import combine_or_conditions
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    arrayElement,
    column,
    literal,
    literals_array,
    not_cond,
    or_cond,
)
from snuba.query.expressions import (
    Argument,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.state.sentry_options import get_mapped_option, get_option
from snuba.web.rpc.common.common import (
    _in_or_has,
    _scalar_value,
    get_field_existence_expression,
    timestamp_seconds_to_datetime_literal,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


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
    names = coalesced_attribute_names(k.name)

    def _value(name: str) -> Expression:
        elem = arrayElement(None, column(col_name), literal(name))
        # ints live in the float map and surface as Int64, so they need a cast.
        if k.type == AttributeKey.Type.TYPE_INT:
            return f.cast(elem, f"Nullable({PROTO_TYPE_TO_CLICKHOUSE_TYPE[k.type]})")
        return elem

    values = [_value(name) for name in names]
    existences = key_existence_conditions(col_name, names)

    exists = combine_or_conditions(existences) if len(existences) > 1 else existences[0]
    value = first_present_value(values, existences)
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


def _comparison_can_match_column_default(v: AttributeValue, value_type: str | None) -> bool:
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


_POSITIVE_OP_FOR_NEGATIVE: dict[
    AnyAttributeFilter.Op.ValueType, AnyAttributeFilter.Op.ValueType
] = {
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


def _array_value_length(v: AttributeValue, value_type: str) -> int:
    """Element count of an array-typed ``AttributeValue``. ``value_type`` must be one of
    ``_ARRAY_VALUE_TYPES``; every one of them wraps a message with a ``values`` field."""
    return len(getattr(v, value_type).values)


_ArrayComparisonValidator = Callable[
    [ComparisonFilter.Op.ValueType, AttributeValue, AttributeKey], None
]


def _require_element_typed_array_key(key: AttributeKey, subject: str) -> None:
    """Exact equality and hasAny/hasAll compare against a single native typed column, which
    the deprecated untyped ``TYPE_ARRAY`` doesn't have."""
    if array_element_column(key) is None:
        raise BadSnubaRPCRequestException(
            f"{subject} only supported on element-typed array keys "
            f"(TYPE_ARRAY_STRING/INT/DOUBLE/BOOL), got {AttributeKey.Type.Name(key.type)}"
        )


def _validate_array_pattern_match(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    label = "REGEXP" if op == ComparisonFilter.OP_REGEXP else "LIKE/NOT_LIKE"
    if v.WhichOneof("value") != "val_str":
        raise BadSnubaRPCRequestException(f"{label} on array keys requires a string pattern")
    # LIKE/REGEXP only match string elements, so they make sense only for string arrays.
    if key.type not in (AttributeKey.Type.TYPE_ARRAY, AttributeKey.Type.TYPE_ARRAY_STRING):
        raise BadSnubaRPCRequestException(
            f"{label} on array keys is only supported on string arrays "
            f"(TYPE_ARRAY_STRING), got {AttributeKey.Type.Name(key.type)}"
        )


def _validate_array_equals(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    """Two modes, dispatched on the RHS type:
    - scalar value -> "any element equals scalar" (includes), for all array key types.
    - array value  -> exact ordered array equality, element-typed keys only.
    Arrays can be empty or non-empty, but never null or with null elements."""
    vt = v.WhichOneof("value")
    if vt in (None, "val_null"):
        raise BadSnubaRPCRequestException(
            "OP_EQUALS/OP_NOT_EQUALS on array keys require a scalar value "
            "(e.g. val_str, val_int) for element membership, or an array value "
            "(e.g. val_str_array) for exact array equality"
        )
    if vt not in _ARRAY_VALUE_TYPES:
        return
    _require_element_typed_array_key(key, "exact array equality (array value) is")
    # An empty array would build an untyped `[]` (Array(Nothing)) RHS, and matching it
    # is indistinguishable from the attribute being absent (arrayElement on a missing
    # map key returns an empty array), so reject it like OP_HAS_ANY/OP_HAS_ALL do.
    if _array_value_length(v, vt) == 0:
        raise BadSnubaRPCRequestException(
            "exact array equality (array value) requires a non-empty array"
        )


def _validate_array_has(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    _require_element_typed_array_key(key, "OP_HAS_ANY/OP_HAS_ALL are")
    vt = v.WhichOneof("value")
    if vt not in _ARRAY_VALUE_TYPES:
        raise BadSnubaRPCRequestException(
            "OP_HAS_ANY/OP_HAS_ALL require an array value (e.g. val_str_array)"
        )
    if _array_value_length(v, vt) == 0:
        raise BadSnubaRPCRequestException("OP_HAS_ANY/OP_HAS_ALL require a non-empty array")


def _reject_array_in(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    # IN/NOT_IN on an array key is the same as "shares any element", which the
    # dedicated array operators express directly, so point the user there.
    raise BadSnubaRPCRequestException(
        "OP_IN/OP_NOT_IN are not supported on array keys; use OP_HAS_ANY "
        "(match any element) or OP_HAS_ALL (match all elements) instead"
    )


_ARRAY_COMPARISON_VALIDATORS: dict[ComparisonFilter.Op.ValueType, _ArrayComparisonValidator] = {
    ComparisonFilter.OP_LIKE: _validate_array_pattern_match,
    ComparisonFilter.OP_NOT_LIKE: _validate_array_pattern_match,
    ComparisonFilter.OP_REGEXP: _validate_array_pattern_match,
    ComparisonFilter.OP_EQUALS: _validate_array_equals,
    ComparisonFilter.OP_NOT_EQUALS: _validate_array_equals,
    ComparisonFilter.OP_HAS_ANY: _validate_array_has,
    ComparisonFilter.OP_HAS_ALL: _validate_array_has,
    ComparisonFilter.OP_IN: _reject_array_in,
    ComparisonFilter.OP_NOT_IN: _reject_array_in,
}


def _validate_comparison_filter_type_array(
    op: ComparisonFilter.Op.ValueType, v: AttributeValue, key: AttributeKey
) -> None:
    validator = _ARRAY_COMPARISON_VALIDATORS.get(op)
    if validator is None:
        raise BadSnubaRPCRequestException(
            f"{ComparisonFilter.Op.Name(op)} is not supported on array keys "
            "(supported: LIKE, NOT_LIKE, REGEXP, OP_EQUALS, OP_NOT_EQUALS, OP_HAS_ANY, "
            "OP_HAS_ALL)"
        )
    validator(op, v, key)


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


def _native_literal_for_array_column(col: str, v: AttributeValue) -> Literal:
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


def _native_literals_array_for_array_column(col: str, v: AttributeValue) -> Expression:
    """The filter's array value coerced element-wise to a single typed array column's native
    element type, as a ``literals_array``, for exact array equality / hasAny / hasAll.

    Mirrors ``_native_literal_for_array_column`` per element: each element must match the
    column's element type (or be a ``val_str`` that parses to it). Accepts the native array
    fields (``val_str_array`` / ``val_int_array`` / ``val_float_array`` / ``val_double_array``)
    and the generic ``val_array`` (whose elements are per-element ``AttributeValue``s)."""
    value_type = v.WhichOneof("value")
    if value_type == "val_array":
        elems = list(v.val_array.values)
    elif value_type == "val_str_array":
        elems = [AttributeValue(val_str=s) for s in v.val_str_array.values]
    elif value_type == "val_int_array":
        elems = [AttributeValue(val_int=i) for i in v.val_int_array.values]
    elif value_type == "val_float_array":
        elems = [AttributeValue(val_float=x) for x in v.val_float_array.values]
    elif value_type == "val_double_array":
        elems = [AttributeValue(val_double=x) for x in v.val_double_array.values]
    else:
        raise BadSnubaRPCRequestException(
            f"array comparison requires an array value, got {value_type}"
        )
    return literals_array(None, [_native_literal_for_array_column(col, e) for e in elems])


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


def _typed_array_exact_equals_expression(attr_key: AttributeKey, v: AttributeValue) -> Expression:
    """Exact ordered array equality against an element-typed array key's single native column:
    ``arrayElement(attributes_array_<t>, 'key') = [<coerced elements>]``. Element-typed keys
    only (validated by ``_validate_comparison_filter_type_array``)."""
    col = array_element_column(attr_key)
    assert col is not None  # element-typed array only (validated upstream)
    array_expr = type_array_typed_column_native_array(attr_key, col)
    rhs = _native_literals_array_for_array_column(col, v)
    return f.equals(array_expr, rhs)


def _typed_array_has_expression(
    attr_key: AttributeKey, v: AttributeValue, function_name: str
) -> Expression:
    """``hasAny`` / ``hasAll`` of an element-typed array key's single native column against the
    coerced filter set: ``hasAny(arrayElement(attributes_array_<t>, 'key'), [<elements>])``.
    ``function_name`` is ``"hasAny"`` or ``"hasAll"``. Element-typed keys only (validated
    upstream)."""
    col = array_element_column(attr_key)
    assert col is not None  # element-typed array only (validated upstream)
    array_expr = type_array_typed_column_native_array(attr_key, col)
    rhs = _native_literals_array_for_array_column(col, v)
    return FunctionCall(None, function_name, (array_expr, rhs))


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


def _regexp_match(value: Expression, pattern: Expression, ignore_case: bool) -> FunctionCall:
    # Needs ClickHouse > 26.8 for matchCaseInsensitive; lower() is a stand-in until then.
    if ignore_case:
        return f.match(f.lower(value), f.lower(pattern))
    return f.match(value, pattern)


def _is_valid_regexp_pattern(v: AttributeValue) -> None:
    if v.WhichOneof("value") != "val_str" or v.val_str == "":
        raise BadSnubaRPCRequestException("REGEXP pattern must be a non-empty string")


def _any_attribute_op_expression(
    *,
    filter_: AnyAttributeFilter,
    op: AnyAttributeFilter.Op.ValueType,
    element: Argument,
    value_expr: Expression,
    membership_as_has: bool,
) -> Expression:
    match op:
        case AnyAttributeFilter.OP_EQUALS:
            if filter_.ignore_case:
                return f.equals(f.lower(element), f.lower(value_expr))
            return f.equals(element, value_expr)
        case AnyAttributeFilter.OP_LIKE:
            if filter_.ignore_case:
                return f.ilike(element, value_expr)
            return f.like(element, value_expr)
        case AnyAttributeFilter.OP_REGEXP:
            return _regexp_match(element, value_expr, filter_.ignore_case)
        case AnyAttributeFilter.OP_IN:
            if filter_.ignore_case:
                if filter_.value.WhichOneof("value") == "val_str_array":
                    lowered = [literal(s.lower()) for s in filter_.value.val_str_array.values]
                else:
                    lowered = [
                        literal(elem.val_str.lower()) for elem in filter_.value.val_array.values
                    ]
                return _in_or_has(
                    f.lower(element),
                    literals_array(None, lowered),
                    as_has=membership_as_has,
                )
            return _in_or_has(element, value_expr, as_has=membership_as_has)
        case _:
            raise BadSnubaRPCRequestException(
                f"Unsupported any_attribute_filter op: {AnyAttributeFilter.Op.Name(filter_.op)}"
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

    # LIKE/NOT_LIKE/REGEXP only makes sense on string columns
    if (
        effective_op in (AnyAttributeFilter.OP_LIKE, AnyAttributeFilter.OP_REGEXP)
        and col_name not in _STRING_COLUMNS
    ):
        label = "REGEXP" if effective_op == AnyAttributeFilter.OP_REGEXP else "LIKE/NOT_LIKE"
        raise BadSnubaRPCRequestException(f"{label} operations are only supported on string values")

    if effective_op == AnyAttributeFilter.OP_REGEXP:
        _is_valid_regexp_pattern(v)

    # ignore_case uses lower() which only works on string columns
    if filt.ignore_case and col_name not in _STRING_COLUMNS:
        raise BadSnubaRPCRequestException("Cannot ignore case on non-string values")

    v_expression = _attribute_value_to_expression(v)

    # 3. Build the lambda comparison
    x = Argument(None, "x")
    comparison = _any_attribute_op_expression(
        filter_=filt,
        op=effective_op,
        element=x,
        value_expr=v_expression,
        membership_as_has=membership_as_has,
    )
    lam = Lambda(None, ("x",), comparison)

    # 4. Build the arrayExists expression for the single matching column.
    positive_expr = f.arrayExists(lam, f.mapValues(column(col_name)))

    if is_negative:
        return not_cond(positive_expr)
    return positive_expr


class IndexedColumn(TypedDict):
    indexed_column_name: str
    indexed_start_date: str


INDEXED_COLUMNS_OPTION = "indexed_columns"


def indexed_column_for(
    start_timestamp: ProtobufTimestamp | None,
    item_type: TraceItemType.ValueType,
    unindexed_column: str,
) -> str | None:
    """
    Indexed columns can't be backfilled, so each one only holds data from its
    indexed_start_date (YYYY-MM-DD) onwards. An indexed column can hold a
    different attribute per item type (indexed_name is sentry.op for spans,
    sentry.metric_name for metrics), so each entry only applies to its own item_type.
    Set per region in options-automator.
    """
    try:
        if start_timestamp is None:
            return None

        indexed_column: dict[str, IndexedColumn] = get_mapped_option(
            INDEXED_COLUMNS_OPTION, unindexed_column, {}
        )

        type = get_trace_item_type_name(item_type)
        indexed_type = indexed_column.get(type)
        if indexed_type is None:
            return None

        indexed_start = datetime.fromisoformat(indexed_type["indexed_start_date"]).replace(
            tzinfo=UTC
        )
        if start_timestamp.ToDatetime(tzinfo=UTC) < indexed_start:
            return None
        return indexed_type["indexed_column_name"]
    except Exception:
        return None


@dataclass(frozen=True)
class TraceItemFilterConverter:
    """Converts a ``TraceItemFilter`` tree into an expression. Holds the settings
    every nested filter needs unchanged; see ``trace_item_filters_to_expression``
    for what each one does."""

    item_type: TraceItemType.ValueType
    attribute_key_to_expression: Callable[[AttributeKey], Expression]
    membership_as_has: bool = False
    start_timestamp: ProtobufTimestamp | None = None

    def to_expression(self, item_filter: TraceItemFilter) -> Expression:
        match item_filter.WhichOneof("value"):
            case "and_filter":
                return self._and(item_filter.and_filter)
            case "or_filter":
                return self._or(item_filter.or_filter)
            case "not_filter":
                return self._not(item_filter.not_filter)
            case "comparison_filter":
                return self._comparison(item_filter.comparison_filter)
            case "exists_filter":
                return self._exists(item_filter.exists_filter)
            case "any_attribute_filter":
                return self._any_attribute(item_filter.any_attribute_filter)
            case _:
                return literal(True)

    def _and(self, and_filter: AndFilter) -> Expression:
        filters = and_filter.filters
        if len(filters) == 0:
            return literal(True)
        if len(filters) == 1:
            return self.to_expression(filters[0])
        return and_cond(*(self.to_expression(x) for x in filters))

    def _or(self, or_filter: OrFilter) -> Expression:
        filters = or_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'or' clause")
        if len(filters) == 1:
            return self.to_expression(filters[0])
        return or_cond(*(self.to_expression(x) for x in filters))

    def _not(self, not_filter: NotFilter) -> Expression:
        filters = not_filter.filters
        if len(filters) == 0:
            raise BadSnubaRPCRequestException("Invalid trace item filter, empty 'not' clause")
        if len(filters) == 1:
            return not_cond(self.to_expression(filters[0]))
        return not_cond(and_cond(*(self.to_expression(x) for x in filters)))

    def _exists(self, exists_filter: ExistsFilter) -> Expression:
        return get_field_existence_expression(
            _trace_item_filter_key_expression(
                attr_to_key_expression_callable=self.attribute_key_to_expression,
                key=exists_filter.key,
            )
        )

    def _any_attribute(self, any_attribute_filter: AnyAttributeFilter) -> Expression:
        if not get_option("enable_any_attribute_filter", True):
            return literal(True)
        return _any_attribute_filter_to_expression(
            any_attribute_filter, membership_as_has=self.membership_as_has
        )

    def _comparison(self, comparison_filter: ComparisonFilter) -> Expression:
        k = comparison_filter.key
        v = comparison_filter.value
        op = comparison_filter.op

        if k.type in ARRAY_TYPES:
            _validate_comparison_filter_type_array(op, v, k)

        k_expression = _trace_item_filter_key_expression(
            attr_to_key_expression_callable=self.attribute_key_to_expression,
            key=k,
        )

        value_type = v.WhichOneof("value")
        if value_type is None:
            raise BadSnubaRPCRequestException("comparison does not have a right hand side")
        v_expression: Expression = (
            literal(None)
            if v.is_null or value_type == "val_null"
            else _attribute_value_to_expression(v)
        )

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
        if k.name == sentry_column("timestamp") and value_type in (
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

        match op:
            case ComparisonFilter.OP_EQUALS:
                return self._equals(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_NOT_EQUALS:
                return self._not_equals(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_LIKE:
                return self._like(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_REGEXP:
                return self._regexp(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_NOT_LIKE:
                return self._not_like(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_LESS_THAN:
                return f.less(k_expression, v_expression)
            case ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
                return f.lessOrEquals(k_expression, v_expression)
            case ComparisonFilter.OP_GREATER_THAN:
                return f.greater(k_expression, v_expression)
            case ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
                return f.greaterOrEquals(k_expression, v_expression)
            case ComparisonFilter.OP_IN:
                return self._in(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_NOT_IN:
                return self._not_in(comparison_filter, k_expression, v_expression)
            case ComparisonFilter.OP_HAS_ANY | ComparisonFilter.OP_HAS_ALL:
                return self._has_any_or_all(comparison_filter)
            case _:
                raise BadSnubaRPCRequestException(
                    f"Invalid string comparison, unknown op: {comparison_filter}"
                )

    def _indexed_column(self, comparison_filter: ComparisonFilter) -> str | None:
        """The bloom-filter-indexed column an equality / IN on this key can use, if any."""
        k = comparison_filter.key
        if (
            k.type != AttributeKey.Type.TYPE_STRING
            or comparison_filter.value.is_null
            or comparison_filter.ignore_case
        ):
            return None
        return indexed_column_for(self.start_timestamp, self.item_type, k.name)

    def _equals(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        k, v, ignore_case = (
            comparison_filter.key,
            comparison_filter.value,
            comparison_filter.ignore_case,
        )
        index_name = self._indexed_column(comparison_filter)
        if index_name is not None and v.val_str:
            return f.equals(column(index_name), v_expression)

        value_type = v.WhichOneof("value")
        _check_non_string_values_cannot_ignore_case(comparison_filter)

        if k.type in ARRAY_TYPES:
            # Array value -> exact ordered array equality (element-typed keys only);
            # scalar value -> "any element equals scalar" (includes).
            if value_type in _ARRAY_VALUE_TYPES:
                if ignore_case:
                    raise BadSnubaRPCRequestException(
                        "ignore_case is not supported for exact array equality"
                    )
                return _typed_array_exact_equals_expression(k, v)
            return _typed_array_includes_scalar_expression(k, v, ignore_case)
        if _is_map_backed_key(k):
            # Map-backed: NULL-free (exists, value) form (see _map_backed_operands).
            value, exists = _map_backed_operands(k)
            if v.is_null or value_type == "val_null":  # `attr = null` <=> key absent
                return not_cond(exists)
            lhs, rhs = (
                (f.lower(value), f.lower(v_expression)) if ignore_case else (value, v_expression)
            )
            cmp = f.equals(lhs, rhs)
            # existence guard only needed when '' / 0 could match an absent key.
            if _comparison_can_match_column_default(v, value_type):
                return and_cond(exists, cmp)
            return cmp
        expr = (
            f.equals(f.lower(k_expression), f.lower(v_expression))
            if ignore_case
            else f.equals(k_expression, v_expression)
        )
        # we redefine the way equals works for nulls
        # now null=null is true
        return or_cond(expr, and_cond(f.isNull(k_expression), f.isNull(v_expression)))

    def _not_equals(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        k, v, ignore_case = (
            comparison_filter.key,
            comparison_filter.value,
            comparison_filter.ignore_case,
        )
        value_type = v.WhichOneof("value")
        _check_non_string_values_cannot_ignore_case(comparison_filter)
        if k.type in ARRAY_TYPES:
            if value_type in _ARRAY_VALUE_TYPES:
                if ignore_case:
                    raise BadSnubaRPCRequestException(
                        "ignore_case is not supported for exact array equality"
                    )
                return not_cond(_typed_array_exact_equals_expression(k, v))
            return not_cond(_typed_array_includes_scalar_expression(k, v, ignore_case))
        if _is_map_backed_key(k):
            # Negation of OP_EQUALS; an absent key is "not equal".
            value, exists = _map_backed_operands(k)
            if v.is_null or value_type == "val_null":  # `attr != null` <=> key present
                return exists
            lhs, rhs = (
                (f.lower(value), f.lower(v_expression)) if ignore_case else (value, v_expression)
            )
            if _comparison_can_match_column_default(v, value_type):
                return not_cond(and_cond(exists, f.equals(lhs, rhs)))
            return f.notEquals(lhs, rhs)
        expr = (
            f.notEquals(f.lower(k_expression), f.lower(v_expression))
            if ignore_case
            else f.notEquals(k_expression, v_expression)
        )
        # we redefine the way not equals works for nulls
        # now null!=null is true
        return or_cond(expr, f.xor(f.isNull(k_expression), f.isNull(v_expression)))

    def _like(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        k, ignore_case = comparison_filter.key, comparison_filter.ignore_case
        if k.type in ARRAY_TYPES:
            return _typed_array_like_expression(k, v_expression, ignore_case)
        if k.type != AttributeKey.Type.TYPE_STRING:
            raise BadSnubaRPCRequestException(
                "the LIKE comparison is only supported on string and array keys"
            )
        comparison_function = f.ilike if ignore_case else f.like
        if _is_map_backed_key(k):
            value, exists = _map_backed_operands(k)
            return and_cond(exists, comparison_function(value, v_expression))
        return comparison_function(k_expression, v_expression)

    def _regexp(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        k, ignore_case = comparison_filter.key, comparison_filter.ignore_case
        _is_valid_regexp_pattern(comparison_filter.value)
        if k.type in ARRAY_TYPES:
            return f.arrayExists(
                Lambda(
                    None,
                    ("x",),
                    _regexp_match(Argument(None, "x"), v_expression, ignore_case),
                ),
                type_array_typed_column_native_array(k, "attributes_array_string"),
            )
        if k.type != AttributeKey.Type.TYPE_STRING:
            raise BadSnubaRPCRequestException(
                "the REGEXP comparison is only supported on string and array keys"
            )
        if _is_map_backed_key(k):
            value, exists = _map_backed_operands(k)
            return and_cond(exists, _regexp_match(value, v_expression, ignore_case))
        return _regexp_match(k_expression, v_expression, ignore_case)

    def _not_like(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        k, ignore_case = comparison_filter.key, comparison_filter.ignore_case
        if k.type in ARRAY_TYPES:
            return not_cond(_typed_array_like_expression(k, v_expression, ignore_case))
        if k.type != AttributeKey.Type.TYPE_STRING:
            raise BadSnubaRPCRequestException(
                "the NOT LIKE comparison is only supported on string and array keys"
            )
        if _is_map_backed_key(k):
            # Negation of OP_LIKE; an absent key is "not like".
            like_fn = f.ilike if ignore_case else f.like
            value, exists = _map_backed_operands(k)
            return not_cond(and_cond(exists, like_fn(value, v_expression)))
        comparison_function = f.notILike if ignore_case else f.notLike
        expr = comparison_function(k_expression, v_expression)
        # we redefine the way not like works for nulls
        # now null not like "%anything%" is true
        return or_cond(expr, f.isNull(k_expression))

    def _membership_values(
        self, comparison_filter: ComparisonFilter, v_expression: Expression
    ) -> Expression:
        """The IN / NOT IN right-hand array, lowercased for ignore_case."""
        if not comparison_filter.ignore_case:
            return v_expression
        v = comparison_filter.value
        if v.WhichOneof("value") == "val_str_array":
            return literals_array(None, [literal(s.lower()) for s in v.val_str_array.values])
        return literals_array(None, [literal(elem.val_str.lower()) for elem in v.val_array.values])

    def _in(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        _check_non_string_values_cannot_ignore_case(comparison_filter)
        k, v, ignore_case = (
            comparison_filter.key,
            comparison_filter.value,
            comparison_filter.ignore_case,
        )
        index_name = self._indexed_column(comparison_filter)
        values = v.val_str_array.values
        if index_name is not None and values and "" not in values:
            return _in_or_has(column(index_name), v_expression, as_has=self.membership_as_has)

        v_expression = self._membership_values(comparison_filter, v_expression)
        # note: v_expression must be an array
        # we redefine the way in works for nulls
        # now null in ['hi', null] is true
        if _is_map_backed_key(k):
            # Map-backed: keep the existence if(...) out of in() (see helper).
            return _analyzer_safe_in_expression(
                k,
                v_expression,
                negated=False,
                ignore_case=ignore_case,
                guard=_comparison_can_match_column_default(v, v.WhichOneof("value")),
                membership_as_has=self.membership_as_has,
            )
        k_expression = f.lower(k_expression) if ignore_case else k_expression
        expr = _in_or_has(k_expression, v_expression, as_has=self.membership_as_has)
        return or_cond(
            expr,
            and_cond(f.isNull(k_expression), f.has(v_expression, literal(None))),
        )

    def _not_in(
        self,
        comparison_filter: ComparisonFilter,
        k_expression: Expression,
        v_expression: Expression,
    ) -> Expression:
        _check_non_string_values_cannot_ignore_case(comparison_filter)
        k, v, ignore_case = (
            comparison_filter.key,
            comparison_filter.value,
            comparison_filter.ignore_case,
        )
        v_expression = self._membership_values(comparison_filter, v_expression)
        # note: v_expression must be an array
        # we redefine the way not in works for nulls
        # now null not in ['hi'] is true
        if _is_map_backed_key(k):
            # Map-backed: keep the existence if(...) out of in() (see helper).
            return _analyzer_safe_in_expression(
                k,
                v_expression,
                negated=True,
                ignore_case=ignore_case,
                guard=_comparison_can_match_column_default(v, v.WhichOneof("value")),
                membership_as_has=self.membership_as_has,
            )
        k_expression = f.lower(k_expression) if ignore_case else k_expression
        expr = not_cond(_in_or_has(k_expression, v_expression, as_has=self.membership_as_has))
        return or_cond(
            expr,
            and_cond(
                f.isNull(k_expression),
                not_cond(f.has(v_expression, literal(None))),
            ),
        )

    def _has_any_or_all(self, comparison_filter: ComparisonFilter) -> Expression:
        # "array attribute_key type only" per the proto: reject non-array keys here;
        # array-typed keys are validated above by _validate_comparison_filter_type_array.
        if comparison_filter.key.type not in ARRAY_TYPES:
            raise BadSnubaRPCRequestException(
                "OP_HAS_ANY/OP_HAS_ALL are only supported on array keys"
            )
        if comparison_filter.ignore_case:
            raise BadSnubaRPCRequestException(
                "ignore_case is not supported for OP_HAS_ANY/OP_HAS_ALL"
            )
        function_name = (
            "hasAny" if comparison_filter.op == ComparisonFilter.OP_HAS_ANY else "hasAll"
        )
        return _typed_array_has_expression(
            comparison_filter.key, comparison_filter.value, function_name
        )


def trace_item_filters_to_expression(
    item_type: TraceItemType.ValueType,
    item_filter: TraceItemFilter,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    membership_as_has: bool = False,
    start_timestamp: ProtobufTimestamp | None = None,
) -> Expression:
    """
    Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
    This maps those filters into an expression which can be used in a WHERE clause
    :param item_type: build one call per item type, each AND-ed with its own
        ``item_type =`` condition (see ``cross_item_queries``).
    :param item_filter:
    :param membership_as_has: build ``IN``/``NOT IN`` membership as ``has(array, x)``
        rather than ``x IN (array)``. Pass ``True`` only when the result lands in a
        SELECT clause / projection / aggregate condition / ``HAVING`` — there a constant
        ``IN`` set leaks an unstable ``__set_*`` identifier into the result-block column
        name and breaks mixed-version distributed reads (see ``_in_or_has``). Leave the
        default for WHERE clauses, where the prepared ``IN`` set drives pruning.
    :param start_timestamp: request start; enables reading indexed columns, see
        ``indexed_column_for``.
    :return:

    Array predicates always read the typed ``attributes_array_*`` map columns: an
    element-typed array key (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL) hits its single column
    natively, the deprecated untyped ``TYPE_ARRAY`` searches all four.
    """
    return TraceItemFilterConverter(
        item_type=item_type,
        attribute_key_to_expression=attribute_key_to_expression,
        membership_as_has=membership_as_has,
        start_timestamp=start_timestamp,
    ).to_expression(item_filter)
