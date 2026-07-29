"""``eap_item_co_occurring_attrs``: the original co-occurring-attributes roll-up.

A ``ReplacingMergeTree`` holding one row per distinct attribute-key set, with only
string/float/bool key arrays. Two consequences shape the queries below:

- **Int keys have no array of their own.** They are visible only because ``eap_items``
  double-writes int attributes into a float bucket, so an int request reads the float array
  and its keys are reported as ``TYPE_DOUBLE``.
- **Array-typed keys are not represented at all**, so an array-typed request cannot be
  answered natively and falls back to reading every scalar array.

There is also no occurrence count, so frequency means "number of matching rows".
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

# Every key array this storage has. Used for TYPE_UNSPECIFIED and, because there is nothing
# better to read, for the array types it cannot answer.
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
            # backwards compatibility with TYPE_FLOAT: same column, echo the requested type
            return [("attributes_float", "TYPE_FLOAT")]
        if requested_type in (AttributeKey.Type.TYPE_DOUBLE, AttributeKey.Type.TYPE_INT):
            return [FLOAT_KEY_ARRAY]
        if requested_type == AttributeKey.Type.TYPE_BOOLEAN:
            return [BOOL_KEY_ARRAY]
        # TYPE_UNSPECIFIED, and the array types this storage cannot answer natively: read
        # everything it has, which is what this endpoint has always done.
        return ALL_KEY_ARRAYS

    def count_expression(self) -> Expression:
        # One row per distinct attribute-key set, so the frequency of a key is the number
        # of those sets it appears in.
        return f.count(alias="count")


V1 = CoOccurringAttrsV1()
