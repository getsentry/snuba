from collections import defaultdict
from typing import Final, Mapping, Sequence

from sentry_conventions.attributes import ATTRIBUTE_METADATA
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import Functions as f
from snuba.query.dsl import arrayElement, column, literal
from snuba.query.expressions import (
    Argument,
    Expression,
    FunctionCall,
    JsonPath,
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


def type_array_to_membership_array_expression(attr_key: AttributeKey) -> FunctionCall:
    """To be used only in WHERE clause, not SELECT"""
    if attr_key.type != AttributeKey.Type.TYPE_ARRAY:
        raise MalformedAttributeException(
            f"type_array_to_membership_array_expression expected TYPE_ARRAY, got "
            f"{AttributeKey.Type.Name(attr_key.type)}"
        )
    # We need different label than attribute_key_to_expression(TYPE_ARRAY) [toJSONString]
    alias = f"{_build_label_mapping_key(attr_key)}__array_members"
    x = Argument(None, "x")
    return FunctionCall(
        alias=alias,
        function_name="arrayMap",
        parameters=(
            Lambda(
                alias=None,
                parameters=("x",),
                transformation=FunctionCall(
                    alias=None,
                    function_name="coalesce",
                    parameters=(
                        JsonPath(None, x, "String", "Nullable(String)"),
                        FunctionCall(
                            None,
                            "toString",
                            (JsonPath(None, x, "Int", "Nullable(Int64)"),),
                        ),
                        FunctionCall(
                            None,
                            "toString",
                            (JsonPath(None, x, "Double", "Nullable(Float64)"),),
                        ),
                        JsonPath(None, x, "Bool", "Nullable(String)"),
                    ),
                ),
            ),
            JsonPath(
                alias=None,
                base=column("attributes_array"),
                path=attr_key.name,
                return_type="Array(JSON)",
            ),
        ),
    )


def type_array_to_stored_array_json_path(attr_key: AttributeKey) -> JsonPath:
    return JsonPath(
        alias=None,
        base=column("attributes_array"),
        path=attr_key.name,
        return_type="Array(JSON)",
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
    """Native ``arrayElement`` read per typed array map column for a TYPE_ARRAY SELECT
    past the cutoff (replaces the legacy ``toJSONString`` JSON-column form). Arrays are
    homogeneous, so one sub-column is non-empty; the caller merges them back into one
    array (``merge_typed_array_subcolumns``). Aliased ``"<label_mapping_key>.<column>"``
    so SELECT and GROUP BY / ORDER BY agree."""
    if attr_key.type != AttributeKey.Type.TYPE_ARRAY:
        raise MalformedAttributeException(
            f"type_array_typed_columns_select_expressions expected TYPE_ARRAY, got "
            f"{AttributeKey.Type.Name(attr_key.type)}"
        )
    label_mapping_key = _build_label_mapping_key(attr_key)
    return [
        arrayElement(f"{label_mapping_key}.{col}", column(col), literal(attr_key.name))
        for col in TYPED_ARRAY_MAP_COLUMNS
    ]


# The typed array map column(s) that can hold a filter value of each AttributeValue
# type. A scalar value can only live in the column for its type, so a membership filter
# only needs to read that column instead of all four. Numbers map to both the int and
# float columns: array ints are written only to ``attributes_array_int`` and floats only
# to ``attributes_array_float`` (see ``AttributeMap::insert_array``), and the query value
# type (``val_int`` vs ``val_double``) does not always match the stored element type.
_VALUE_TYPE_TO_ARRAY_MEMBERSHIP_COLUMNS: dict[str, tuple[str, ...]] = {
    "val_str": ("attributes_array_string",),
    "val_int": ("attributes_array_int", "attributes_array_float"),
    "val_float": ("attributes_array_int", "attributes_array_float"),
    "val_double": ("attributes_array_int", "attributes_array_float"),
    "val_bool": ("attributes_array_bool",),
}


def type_array_to_membership_array_expression_from_typed_columns(
    attr_key: AttributeKey,
    value_type: str | None = None,
) -> FunctionCall:
    """WHERE-clause membership array built from the typed array map columns.

    Counterpart to ``type_array_to_membership_array_expression``, which reads the
    legacy ``attributes_array`` JSON column. Since 2026-06-22 array attributes are
    also double-written into typed ``Map(String, Array(T))`` columns; for query
    windows new enough that those columns are fully populated (see
    ``use_array_map_columns``) we read them instead.

    Returns a normalized ``Array(String)`` of the array's elements so the per-element
    comparisons built by ``_type_array_membership_rhs_expression`` keep matching the
    JSON-column behaviour (string elements stay as-is, numbers become ``toString(...)``,
    and bools become ``'true'``/``'false'``).

    ``value_type`` is the ``AttributeValue`` oneof of the value being compared against
    (e.g. ``"val_str"``). A scalar can only live in the typed column(s) for its type, so
    we read just those (see ``_VALUE_TYPE_TO_ARRAY_MEMBERSHIP_COLUMNS``) instead of all
    four. ``None`` (e.g. an exists filter, which has no value) reads every column.
    """
    if attr_key.type != AttributeKey.Type.TYPE_ARRAY:
        raise MalformedAttributeException(
            f"type_array_to_membership_array_expression_from_typed_columns expected "
            f"TYPE_ARRAY, got {AttributeKey.Type.Name(attr_key.type)}"
        )
    alias = f"{_build_label_mapping_key(attr_key)}__array_members"
    name = attr_key.name

    def _normalized_elements(col_name: str) -> FunctionCall:
        elements = arrayElement(None, column(col_name), literal(name))
        if col_name == "attributes_array_string":
            return elements  # already Array(String)
        x = Argument(None, "x")
        if col_name == "attributes_array_bool":
            transform: FunctionCall = FunctionCall(
                None, "if", (x, literal("true"), literal("false"))
            )
        else:
            transform = FunctionCall(None, "toString", (x,))
        return FunctionCall(None, "arrayMap", (Lambda(None, ("x",), transform), elements))

    columns = _VALUE_TYPE_TO_ARRAY_MEMBERSHIP_COLUMNS.get(value_type or "", TYPED_ARRAY_MAP_COLUMNS)
    return FunctionCall(
        alias=alias,
        function_name="arrayConcat",
        parameters=tuple(_normalized_elements(col) for col in columns),
    )


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
        else:
            return _generate_subscriptable_reference(
                attr_key.name,
                attr_key.type,
                alias,
            )

    if attr_key.type == AttributeKey.Type.TYPE_ARRAY:
        # Legacy JSON column (used pre-cutoff and for aggregations); the typed-column read
        # path is built separately (type_array_typed_columns_select_expressions).
        return FunctionCall(
            alias=alias,
            function_name="toJSONString",
            parameters=(type_array_to_stored_array_json_path(attr_key),),
        )

    raise MalformedAttributeException(
        f"Attribute {attr_key.name} has an unknown type: {AttributeKey.Type.Name(attr_key.type)}"
    )
