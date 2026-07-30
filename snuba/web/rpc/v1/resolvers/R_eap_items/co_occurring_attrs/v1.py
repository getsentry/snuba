"""``eap_item_co_occurring_attrs``: the original co-occurring-attributes roll-up.

A ``ReplacingMergeTree`` holding one row per distinct attribute-key set, with only
string/float/bool key arrays and no occurrence count. So int keys have no array of their own
(they are visible only because ``eap_items`` double-writes them into a float bucket) and
array-typed keys are absent entirely.
"""

from __future__ import annotations

from collections.abc import Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.dsl import Functions as f
from snuba.query.expressions import Expression
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
)

CO_OCCURRING_ATTRS_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs")

STRING_KEY_ARRAY = ("attributes_string", "TYPE_STRING")
FLOAT_KEY_ARRAY = ("attributes_float", "TYPE_DOUBLE")
BOOL_KEY_ARRAY = ("attributes_bool", "TYPE_BOOLEAN")

ALL_KEY_ARRAYS: list[tuple[str, str]] = [
    STRING_KEY_ARRAY,
    FLOAT_KEY_ARRAY,
    BOOL_KEY_ARRAY,
]


class CoOccurringAttrsV1(CoOccurringAttrsSource):
    @property
    def storage_key(self) -> StorageKey:
        return CO_OCCURRING_ATTRS_STORAGE_KEY

    def typed_key_arrays(
        self, requested_type: AttributeKey.Type.ValueType
    ) -> Sequence[tuple[str, str]]:
        if requested_type == AttributeKey.Type.TYPE_STRING:
            return [STRING_KEY_ARRAY]
        if requested_type == AttributeKey.Type.TYPE_FLOAT:
            # backwards compatibility: same column, echo the requested type
            return [("attributes_float", "TYPE_FLOAT")]
        if requested_type in (AttributeKey.Type.TYPE_DOUBLE, AttributeKey.Type.TYPE_INT):
            return [FLOAT_KEY_ARRAY]
        if requested_type == AttributeKey.Type.TYPE_BOOLEAN:
            return [BOOL_KEY_ARRAY]
        # TYPE_UNSPECIFIED, and the array types this storage cannot answer natively: read
        # everything it has, which is what this endpoint has always done.
        return ALL_KEY_ARRAYS

    def count_expression(self) -> Expression:
        # Rows are distinct attribute-key sets, so this counts the sets a key appears in.
        return f.count(alias="count")


V1 = CoOccurringAttrsV1()
