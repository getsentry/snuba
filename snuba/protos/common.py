from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Final

from sentry_conventions.attributes import ATTRIBUTE_METADATA
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayElement, column, literal
from snuba.query.expressions import (
    Argument,
    Expression,
    FunctionCall,
    Lambda,
    SubscriptableReference,
)


class MalformedAttributeException(Exception):
    """Exception raised when an AttributeKey proto is malformed or invalid.

    This exception is HTTP-agnostic and should be caught and wrapped by
    HTTP-aware code paths that need to return appropriate status codes.
    """

    pass


COLUMN_PREFIX: str = "sentry."

NORMALIZED_COLUMNS_EAP_ITEMS: Final[Mapping[str, Sequence[AttributeKey.Type.ValueType]]] = {
    f"{COLUMN_PREFIX}organization_id": [AttributeKey.Type.TYPE_INT],
    f"{COLUMN_PREFIX}project_id": [AttributeKey.Type.TYPE_INT],
    f"{COLUMN_PREFIX}timestamp": [
        AttributeKey.Type.TYPE_FLOAT,
        AttributeKey.Type.TYPE_DOUBLE,
        AttributeKey.Type.TYPE_INT,
        AttributeKey.Type.TYPE_STRING,
    ],
    f"{COLUMN_PREFIX}trace_id": [
        AttributeKey.Type.TYPE_STRING
    ],  # this gets converted from a uuid to a string in a storage processor
    f"{COLUMN_PREFIX}item_id": [AttributeKey.Type.TYPE_STRING],
    f"{COLUMN_PREFIX}sampling_weight": [AttributeKey.Type.TYPE_DOUBLE],
    f"{COLUMN_PREFIX}sampling_factor": [AttributeKey.Type.TYPE_DOUBLE],
}

PROTO_TYPE_TO_CLICKHOUSE_TYPE: Final[Mapping[AttributeKey.Type.ValueType, str]] = {
    AttributeKey.Type.TYPE_INT: "Int64",
    AttributeKey.Type.TYPE_STRING: "String",
    AttributeKey.Type.TYPE_DOUBLE: "Float64",
    AttributeKey.Type.TYPE_FLOAT: "Float64",
    AttributeKey.Type.TYPE_BOOLEAN: "Boolean",
}

PROTO_TYPE_TO_ATTRIBUTE_COLUMN: Final[Mapping[AttributeKey.Type.ValueType, str]] = {
    AttributeKey.Type.TYPE_INT: "attributes_float",
    AttributeKey.Type.TYPE_STRING: "attributes_string",
    AttributeKey.Type.TYPE_DOUBLE: "attributes_float",
    AttributeKey.Type.TYPE_FLOAT: "attributes_float",
    AttributeKey.Type.TYPE_BOOLEAN: "attributes_bool",
}

# Element-typed array attribute types map 1:1 to a single typed array map column, so a
# query on one of these reads exactly that column natively (no cross-column OR/merge).
PROTO_ARRAY_TYPE_TO_COLUMN: Final[Mapping[AttributeKey.Type.ValueType, str]] = {
    AttributeKey.Type.TYPE_ARRAY_STRING: "attributes_array_string",
    AttributeKey.Type.TYPE_ARRAY_INT: "attributes_array_int",
    AttributeKey.Type.TYPE_ARRAY_DOUBLE: "attributes_array_float",
    AttributeKey.Type.TYPE_ARRAY_BOOL: "attributes_array_bool",
}

# Every array-typed AttributeKey type, including the deprecated untyped TYPE_ARRAY (which
# has no element type, so its callers must read all four typed columns).
ARRAY_TYPES: Final[frozenset[AttributeKey.Type.ValueType]] = frozenset(
    {AttributeKey.Type.TYPE_ARRAY, *PROTO_ARRAY_TYPE_TO_COLUMN}
)


def array_element_column(attr_key: AttributeKey) -> str | None:
    """The single typed array column for an element-typed array key
    (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL), or None for the deprecated untyped TYPE_ARRAY."""
    return PROTO_ARRAY_TYPE_TO_COLUMN.get(attr_key.type)


def _resolve_canonical(name: str) -> str:
    visited: set[str] = set()
    current = name
    while current in ATTRIBUTE_METADATA:
        meta = ATTRIBUTE_METADATA[current]
        if not meta.deprecation or not meta.deprecation.replacement:
            return current
        if current in visited:
            return current
        visited.add(current)
        current = meta.deprecation.replacement
    return current


def _build_deprecated_attributes() -> dict[str, list[str]]:
    groups: dict[str, set[str]] = defaultdict(set)
    for name, metadata in ATTRIBUTE_METADATA.items():
        if metadata.deprecation and metadata.deprecation.replacement:
            canonical = _resolve_canonical(name)
            groups[canonical].add(name)

    result: dict[str, list[str]] = {}
    for canonical, deprecated_names in groups.items():
        full_group = {canonical} | deprecated_names
        for member in full_group:
            others = full_group - {member}
            if member == canonical:
                result[member] = sorted(others)
            else:
                result[member] = [canonical] + sorted(others - {canonical})
    return result


ATTRIBUTES_TO_COALESCE: dict[str, list[str]] = _build_deprecated_attributes()


def _build_label_mapping_key(attribute_key: AttributeKey) -> str:
    return f"{attribute_key.name}_{AttributeKey.Type.Name(attribute_key.type)}"


def _generate_subscriptable_reference(
    attribute_name: str,
    attribute_type: AttributeKey.Type.ValueType,
    alias: str | None = None,
) -> SubscriptableReference | FunctionCall:
    kwargs = {}
    if alias:
        kwargs["alias"] = alias
    clickhouse_type = PROTO_TYPE_TO_CLICKHOUSE_TYPE[attribute_type]
    if attribute_type == AttributeKey.Type.TYPE_BOOLEAN:
        # Boolean attributes use a Map column without hash buckets,
        # so we use arrayElement directly instead of SubscriptableReference
        # which would be transformed to the nested .key/.value pattern.
        return f.cast(
            arrayElement(
                None,
                column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
                literal(attribute_name),
            ),
            f"Nullable({clickhouse_type})",
            **kwargs,
        )
    if attribute_type == AttributeKey.Type.TYPE_INT:
        return f.cast(
            SubscriptableReference(
                column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
                key=literal(attribute_name),
                alias=None,
            ),
            f"Nullable({clickhouse_type})",
            **kwargs,
        )
    return SubscriptableReference(
        column=column(PROTO_TYPE_TO_ATTRIBUTE_COLUMN[attribute_type]),
        key=literal(attribute_name),
        alias=alias,
    )


# The typed array map columns (Map(String, Array(T))), in element-type order. Shared by
# the per-attribute SELECT (type_array_typed_columns_select_expressions) and the
# whole-map reads / merges in snuba.web.rpc.common.common.
TYPED_ARRAY_MAP_COLUMNS: tuple[str, ...] = (
    "attributes_array_string",
    "attributes_array_int",
    "attributes_array_float",
    "attributes_array_bool",
)


def type_array_typed_columns_select_expressions(attr_key: AttributeKey) -> list[FunctionCall]:
    """Native ``arrayElement`` read per typed array map column, for a deprecated untyped
    ``TYPE_ARRAY`` SELECT whose element type is unknown. Arrays are homogeneous, so one
    sub-column is non-empty; the caller merges them back into one array
    (``merge_typed_array_subcolumns``). Aliased ``"<label_mapping_key>.<column>"`` so
    SELECT and GROUP BY / ORDER BY agree. Element-typed array keys read a single column
    directly (``type_array_typed_element_column_native_array``)."""
    if attr_key.type not in ARRAY_TYPES:
        raise MalformedAttributeException(
            f"type_array_typed_columns_select_expressions expected an array type, got "
            f"{AttributeKey.Type.Name(attr_key.type)}"
        )
    label_mapping_key = _build_label_mapping_key(attr_key)
    return [
        arrayElement(f"{label_mapping_key}.{col}", column(col), literal(attr_key.name))
        for col in TYPED_ARRAY_MAP_COLUMNS
    ]


def type_array_to_membership_array_expression_from_typed_columns(
    attr_key: AttributeKey,
) -> FunctionCall:
    """WHERE-clause membership array for a deprecated untyped ``TYPE_ARRAY`` key, whose
    element type is unknown, so all four typed ``Map(String, Array(T))`` columns must be
    read and concatenated.

    Returns a normalized ``Array(String)`` of every element across all four typed
    columns so value-less per-element comparisons match across types (string elements
    stay as-is, numbers become ``toString(...)``, and bools become
    ``'true'``/``'false'``). Array integers are written only to ``attributes_array_int``
    (not also to the float column — see ``AttributeMap::insert_array`` in the
    ``eap_items`` Rust processor), so the int column must be read for int arrays to
    match; element types never overlap across columns, so no element is duplicated.
    Element-typed array keys instead read their single column natively
    (``type_array_typed_element_column_native_array``).
    """
    if attr_key.type not in ARRAY_TYPES:
        raise MalformedAttributeException(
            f"type_array_to_membership_array_expression_from_typed_columns expected "
            f"an array type, got {AttributeKey.Type.Name(attr_key.type)}"
        )
    alias = f"{_build_label_mapping_key(attr_key)}__array_members"
    name = attr_key.name

    def _to_string_elements(col_name: str) -> FunctionCall:
        x = Argument(None, "x")
        return FunctionCall(
            None,
            "arrayMap",
            (
                Lambda(None, ("x",), FunctionCall(None, "toString", (x,))),
                arrayElement(None, column(col_name), literal(name)),
            ),
        )

    string_elements = arrayElement(None, column("attributes_array_string"), literal(name))
    int_elements = _to_string_elements("attributes_array_int")
    float_elements = _to_string_elements("attributes_array_float")
    bool_x = Argument(None, "x")
    bool_elements = FunctionCall(
        None,
        "arrayMap",
        (
            Lambda(
                None,
                ("x",),
                FunctionCall(None, "if", (bool_x, literal("true"), literal("false"))),
            ),
            arrayElement(None, column("attributes_array_bool"), literal(name)),
        ),
    )
    return FunctionCall(
        alias=alias,
        function_name="arrayConcat",
        parameters=(string_elements, int_elements, float_elements, bool_elements),
    )


def type_array_typed_column_native_array(attr_key: AttributeKey, col: str) -> FunctionCall:
    """Native ``Array(T)`` of one typed array map column's elements, for an array
    membership comparison. The caller compares against the filter value coerced to this
    column's native type, with no string conversion — unlike
    ``type_array_to_membership_array_expression_from_typed_columns``, which normalizes
    every column to ``Array(String)`` for the value-less exists/notEmpty check."""
    if attr_key.type not in ARRAY_TYPES:
        raise MalformedAttributeException(
            f"type_array_typed_column_native_array expected an array type, got "
            f"{AttributeKey.Type.Name(attr_key.type)}"
        )
    alias = f"{_build_label_mapping_key(attr_key)}__array_members_{col}"
    return arrayElement(alias, column(col), literal(attr_key.name))


def type_array_typed_element_column_native_array(attr_key: AttributeKey) -> FunctionCall:
    """Native ``Array(T)`` read of an element-typed array key's single column
    (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL), aliased with the label-mapping key. This is the
    whole read path for such a key: one column, natively typed, no cross-column concat."""
    col = PROTO_ARRAY_TYPE_TO_COLUMN.get(attr_key.type)
    if col is None:
        raise MalformedAttributeException(
            f"type_array_typed_element_column_native_array expected an element-typed array "
            f"type, got {AttributeKey.Type.Name(attr_key.type)}"
        )
    alias = _build_label_mapping_key(attr_key)
    return arrayElement(alias, column(col), literal(attr_key.name))


def attribute_key_to_expression(attr_key: AttributeKey) -> Expression:
    """Convert an AttributeKey proto to a Snuba Expression.

    Raises:
        MalformedAttributeException: If the attribute key is invalid or malformed.
    """
    if attr_key.type == AttributeKey.Type.TYPE_UNSPECIFIED:
        raise MalformedAttributeException(
            f"attribute key {attr_key.name} must have a type specified"
        )

    alias = _build_label_mapping_key(attr_key)

    if attr_key.name == "attr_key":
        return column("attr_key")

    if attr_key.name in NORMALIZED_COLUMNS_EAP_ITEMS:
        if attr_key.type not in NORMALIZED_COLUMNS_EAP_ITEMS[attr_key.name]:
            formatted_attribute_types = ", ".join(
                map(
                    AttributeKey.Type.Name,
                    NORMALIZED_COLUMNS_EAP_ITEMS[attr_key.name],
                )
            )
            raise MalformedAttributeException(
                f"Attribute {attr_key.name} must be one of [{formatted_attribute_types}], got {AttributeKey.Type.Name(attr_key.type)}"
            )

        return f.cast(
            column(attr_key.name[len(COLUMN_PREFIX) :]),
            PROTO_TYPE_TO_CLICKHOUSE_TYPE[attr_key.type],
            alias=alias,
        )

    if attr_key.type in PROTO_TYPE_TO_ATTRIBUTE_COLUMN:
        if attr_key.name in ATTRIBUTES_TO_COALESCE:
            expressions = [
                _generate_subscriptable_reference(
                    attribute_name,
                    attr_key.type,
                )
                for attribute_name in [
                    attr_key.name,
                ]
                + list(ATTRIBUTES_TO_COALESCE[attr_key.name])
            ]
            return f.coalesce(
                *expressions,
                alias=alias,
            )
        return _generate_subscriptable_reference(
            attr_key.name,
            attr_key.type,
            alias,
        )

    if attr_key.type in PROTO_ARRAY_TYPE_TO_COLUMN:
        # Element-typed array key: read its single typed column natively.
        return type_array_typed_element_column_native_array(attr_key)

    if attr_key.type == AttributeKey.Type.TYPE_ARRAY:
        # Deprecated untyped array: element type unknown, so concatenate all four typed
        # columns (normalized to Array(String)). Element-typed keys take the branch above.
        return type_array_to_membership_array_expression_from_typed_columns(attr_key)

    raise MalformedAttributeException(
        f"Attribute {attr_key.name} has an unknown type: {AttributeKey.Type.Name(attr_key.type)}"
    )
