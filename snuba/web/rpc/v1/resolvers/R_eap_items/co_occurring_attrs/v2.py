"""``eap_item_co_occurring_attrs_v2``: the per-type co-occurring-attributes roll-up.

A ``SummingMergeTree`` with one attribute-key array per type (mirroring the typed maps on
``eap_items``, so int and array-typed keys keep their real ``AttributeKey`` type), a ``count``
summed on merge, and a ``last_seen`` timestamp.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import Expression
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
)

CO_OCCURRING_ATTRS_V2_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs_v2")

TYPE_KEY_ARRAYS: Mapping[AttributeKey.Type.ValueType, tuple[str, str]] = {
    AttributeKey.Type.TYPE_STRING: ("attributes_string", "TYPE_STRING"),
    # backwards compatibility: same column as TYPE_DOUBLE, echo the requested type
    AttributeKey.Type.TYPE_FLOAT: ("attributes_float", "TYPE_FLOAT"),
    AttributeKey.Type.TYPE_DOUBLE: ("attributes_float", "TYPE_DOUBLE"),
    AttributeKey.Type.TYPE_INT: ("attributes_int", "TYPE_INT"),
    AttributeKey.Type.TYPE_BOOLEAN: ("attributes_bool", "TYPE_BOOLEAN"),
    AttributeKey.Type.TYPE_ARRAY_STRING: ("attributes_array_string", "TYPE_ARRAY_STRING"),
    AttributeKey.Type.TYPE_ARRAY_INT: ("attributes_array_int", "TYPE_ARRAY_INT"),
    AttributeKey.Type.TYPE_ARRAY_DOUBLE: ("attributes_array_float", "TYPE_ARRAY_DOUBLE"),
    AttributeKey.Type.TYPE_ARRAY_BOOL: ("attributes_array_bool", "TYPE_ARRAY_BOOL"),
}

# The deprecated untyped TYPE_ARRAY has no element type, so it surfaces all four.
ARRAY_KEY_ARRAYS: list[tuple[str, str]] = [
    TYPE_KEY_ARRAYS[attr_type]
    for attr_type in (
        AttributeKey.Type.TYPE_ARRAY_STRING,
        AttributeKey.Type.TYPE_ARRAY_INT,
        AttributeKey.Type.TYPE_ARRAY_DOUBLE,
        AttributeKey.Type.TYPE_ARRAY_BOOL,
    )
]

# `attributes_int` is deliberately left out: int attributes are double-written to a float
# bucket on eap_items, so including both arrays would emit each int key twice (as TYPE_DOUBLE
# and TYPE_INT). An explicit TYPE_INT request reads `attributes_int`.
UNSPECIFIED_KEY_ARRAYS: list[tuple[str, str]] = [
    TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_STRING],
    TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_DOUBLE],
    TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_BOOLEAN],
    *ARRAY_KEY_ARRAYS,
]


class CoOccurringAttrsV2(CoOccurringAttrsSource):
    @property
    def storage_key(self) -> StorageKey:
        return CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def typed_key_arrays(
        self, requested_type: AttributeKey.Type.ValueType
    ) -> Sequence[tuple[str, str]]:
        if requested_type == AttributeKey.Type.TYPE_ARRAY:
            return ARRAY_KEY_ARRAYS
        typed = TYPE_KEY_ARRAYS.get(requested_type)
        if typed is not None:
            return [typed]
        # TYPE_UNSPECIFIED, or any type with no dedicated column
        return UNSPECIFIED_KEY_ARRAYS

    def count_expression(self) -> Expression:
        # Summing the per-row counts approximates the number of items a key was seen on.
        return f.sum(column("count"), alias="count")

    @property
    def has_last_seen(self) -> bool:
        return True

    def last_seen_expression(self) -> Expression:
        return f.max(column("last_seen"), alias="last_seen")


V2 = CoOccurringAttrsV2()
