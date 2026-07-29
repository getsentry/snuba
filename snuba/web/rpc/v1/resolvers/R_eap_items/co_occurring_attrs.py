"""Storage selection and per-storage query shape for the co-occurring attributes tables.

``TraceItemAttributeNames`` answers "which attribute keys co-occur with these ones" from a
pre-aggregated roll-up of ``eap_items`` rather than from the item table itself. There are
two such roll-ups, and they do not have the same schema, so picking one also decides part
of the query's shape:

``eap_item_co_occurring_attrs`` (v1)
    ``ReplacingMergeTree``, one row per distinct attribute-key set. Only has
    string/float/bool key arrays: int keys are visible only because they are
    double-written to a float bucket on ``eap_items``, and array-typed keys are not
    represented at all. Has no occurrence count, so frequency means "number of matching
    rows".

``eap_item_co_occurring_attrs_v2`` (v2)
    ``SummingMergeTree`` with one key array per attribute type and a ``count`` column
    summed on merge. Int and array-typed keys are surfaced with their real
    ``AttributeKey`` type, and frequency is the summed count, which approximates the
    number of items a key was seen on.

The differences are confined to this module so the endpoint can ask for a source and build
one query, instead of branching on storage version at each point where the two diverge.
This cannot be pushed further down into the entity layer: a ``QueryStorageSelector`` picks
a table for an otherwise-identical query, whereas these two need different SELECT lists and
a different aggregate. See ``_typed_key_arrays`` and ``count_expression``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Storage
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column
from snuba.query.expressions import Expression
from snuba.state.sentry_options import get_option
from snuba.web.rpc.common.common import prev_monday

CO_OCCURRING_ATTRS_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs")
CO_OCCURRING_ATTRS_V2_STORAGE_KEY = StorageKey("eap_item_co_occurring_attrs_v2")

# Killswitch-style rollout flag: flip on to allow reading v2, flip back to fall in behind
# v1. Enabling it is not sufficient on its own — a request must also be fully inside the
# window v2 has data for, see for_request.
CO_OCCURRING_ATTRS_V2_OPTION = "use_co_occurring_attrs_v2"

# Unix timestamp of the first `date` bucket the v2 tables hold data for. The v2 tables and
# their materialized view were created on 2026-07-29, and the view only appends buckets
# from when it started running, so v2 has nothing before the Monday of that week
# (2026-07-27 00:00 UTC). `date` is bucketed weekly with toMonday(), and the query rounds
# its lower bound down to the previous Monday, so the cutoff has to be a Monday too:
# anything later would make a request starting mid-week round below the cutoff and read
# a bucket that only exists in v1.
#
# A request reaching back before this reads v1 instead, otherwise the attributes that only
# existed in the earlier part of its range would silently disappear from the results.
# There is no backfill planned, so this stays until v1 is retired.
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION = "co_occurring_attrs_v2_start_timestamp"
CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT = 1785110400  # 2026-07-27 00:00:00 UTC

# (key-array column, AttributeKey type name) per attribute type on the v2 storage, which
# stores one key array per type so each key is surfaced with its own type.
V2_TYPE_KEY_ARRAYS: Mapping[AttributeKey.Type.ValueType, tuple[str, str]] = {
    AttributeKey.Type.TYPE_STRING: ("attributes_string", "TYPE_STRING"),
    # backwards compatibility with TYPE_FLOAT: same column, the requested type name
    AttributeKey.Type.TYPE_FLOAT: ("attributes_float", "TYPE_FLOAT"),
    AttributeKey.Type.TYPE_DOUBLE: ("attributes_float", "TYPE_DOUBLE"),
    AttributeKey.Type.TYPE_INT: ("attributes_int", "TYPE_INT"),
    AttributeKey.Type.TYPE_BOOLEAN: ("attributes_bool", "TYPE_BOOLEAN"),
    AttributeKey.Type.TYPE_ARRAY_STRING: ("attributes_array_string", "TYPE_ARRAY_STRING"),
    AttributeKey.Type.TYPE_ARRAY_INT: ("attributes_array_int", "TYPE_ARRAY_INT"),
    AttributeKey.Type.TYPE_ARRAY_DOUBLE: ("attributes_array_float", "TYPE_ARRAY_DOUBLE"),
    AttributeKey.Type.TYPE_ARRAY_BOOL: ("attributes_array_bool", "TYPE_ARRAY_BOOL"),
}

# The deprecated untyped TYPE_ARRAY has no element type, so it surfaces the keys of all
# four element-typed array maps, each tagged with its own type.
V2_ARRAY_KEY_ARRAYS: list[tuple[str, str]] = [
    V2_TYPE_KEY_ARRAYS[attr_type]
    for attr_type in (
        AttributeKey.Type.TYPE_ARRAY_STRING,
        AttributeKey.Type.TYPE_ARRAY_INT,
        AttributeKey.Type.TYPE_ARRAY_DOUBLE,
        AttributeKey.Type.TYPE_ARRAY_BOOL,
    )
]

# TYPE_UNSPECIFIED surfaces every type. `attributes_int` is deliberately left out: int
# attributes are double-written to a float bucket on eap_items, so they are already in
# `attributes_float` and including both arrays would emit each int key twice (once as
# TYPE_DOUBLE, once as TYPE_INT). An explicit TYPE_INT request reads `attributes_int`.
V2_UNSPECIFIED_KEY_ARRAYS: list[tuple[str, str]] = [
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_STRING],
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_DOUBLE],
    V2_TYPE_KEY_ARRAYS[AttributeKey.Type.TYPE_BOOLEAN],
    *V2_ARRAY_KEY_ARRAYS,
]

# v1's key arrays, which cover only the three scalar types it stores.
V1_STRING_KEY_ARRAY = ("attributes_string", "TYPE_STRING")
V1_FLOAT_KEY_ARRAY = ("attributes_float", "TYPE_DOUBLE")
V1_BOOL_KEY_ARRAY = ("attributes_bool", "TYPE_BOOLEAN")
V1_ALL_KEY_ARRAYS: list[tuple[str, str]] = [
    V1_STRING_KEY_ARRAY,
    V1_FLOAT_KEY_ARRAY,
    V1_BOOL_KEY_ARRAY,
]


class CoOccurringAttrsSource:
    """One of the co-occurring-attributes storages, plus the query shape it requires.

    Instances are the module-level ``V1``/``V2`` singletons; use ``for_request`` to get the
    one a request should read.
    """

    def __init__(self, storage_key: StorageKey, *, per_type_key_arrays: bool) -> None:
        self.storage_key = storage_key
        # True when the storage keeps one attribute-key array per type (v2), False when
        # only the three scalar arrays exist (v1).
        self.per_type_key_arrays = per_type_key_arrays

    def __repr__(self) -> str:
        return f"CoOccurringAttrsSource({self.storage_key.value})"

    @property
    def data_source(self) -> Storage:
        """The query's FROM clause."""
        return Storage(
            key=self.storage_key,
            schema=get_storage(self.storage_key).get_schema().get_columns(),
            sample=None,
        )

    def typed_key_arrays(
        self, requested_type: AttributeKey.Type.ValueType
    ) -> Sequence[tuple[str, str]]:
        """The (key-array column, ``AttributeKey`` type name) pairs a request's ``type``
        reads on this storage.

        The type name is what the response reports the key as, so it follows the column the
        key was actually read from: on v1 an int request reads the float array and its keys
        come back ``TYPE_DOUBLE``, while on v2 the same request reads ``attributes_int``
        and they come back ``TYPE_INT``.
        """
        if not self.per_type_key_arrays:
            if requested_type == AttributeKey.Type.TYPE_STRING:
                return [V1_STRING_KEY_ARRAY]
            if requested_type == AttributeKey.Type.TYPE_FLOAT:
                # backwards compatibility with TYPE_FLOAT: echo the requested type
                return [("attributes_float", "TYPE_FLOAT")]
            if requested_type in (AttributeKey.Type.TYPE_DOUBLE, AttributeKey.Type.TYPE_INT):
                return [V1_FLOAT_KEY_ARRAY]
            if requested_type == AttributeKey.Type.TYPE_BOOLEAN:
                return [V1_BOOL_KEY_ARRAY]
            # TYPE_UNSPECIFIED, and the array types v1 cannot answer natively: fall back to
            # every array it has, which is what this endpoint has always done.
            return V1_ALL_KEY_ARRAYS

        if requested_type == AttributeKey.Type.TYPE_ARRAY:
            # deprecated untyped TYPE_ARRAY: no element type, so surface all four
            return V2_ARRAY_KEY_ARRAYS
        typed = V2_TYPE_KEY_ARRAYS.get(requested_type)
        if typed is not None:
            return [typed]
        # TYPE_UNSPECIFIED (or any type with no dedicated column) surfaces every type
        return V2_UNSPECIFIED_KEY_ARRAYS

    def key_array_columns(self, requested_type: AttributeKey.Type.ValueType) -> list[str]:
        """Just the column names from ``typed_key_arrays``, for the row prefilter."""
        return [col for col, _ in self.typed_key_arrays(requested_type)]

    def count_expression(self) -> Expression:
        """How often each key occurs, for frequency ordering.

        v1 has one row per distinct attribute set, so this counts rows. v2 rows each carry
        an occurrence ``count`` the SummingMergeTree accumulates, so summing it approximates
        the number of items the key was seen on.
        """
        if self.per_type_key_arrays:
            return f.sum(column("count"), alias="count")
        return f.count(alias="count")


V1 = CoOccurringAttrsSource(CO_OCCURRING_ATTRS_STORAGE_KEY, per_type_key_arrays=False)
V2 = CoOccurringAttrsSource(CO_OCCURRING_ATTRS_V2_STORAGE_KEY, per_type_key_arrays=True)


def _v2_covers_request_window(request: TraceItemAttributeNamesRequest) -> bool:
    """Whether v2 has data for the whole time range the request asks about.

    The comparison is against the request's *rounded* lower bound rather than the raw start
    timestamp, because that is the bucket the query actually reads: a request starting
    Wednesday reads from the Monday of that week (see
    ``get_co_occurring_attributes_date_condition``). Comparing the raw timestamp would let a
    request starting just after the cutoff read the preceding, non-existent bucket.
    """
    start_timestamp = get_option(
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    )
    earliest_bucket = prev_monday(
        request.meta.start_timestamp.ToDatetime().replace(hour=0, minute=0, second=0)
    )
    # ToDatetime() returns a naive UTC datetime, so drop the tzinfo to compare like-for-like
    v2_start = datetime.fromtimestamp(start_timestamp, UTC).replace(tzinfo=None)
    return earliest_bucket >= v2_start


def for_request(request: TraceItemAttributeNamesRequest) -> CoOccurringAttrsSource:
    """The co-occurring-attributes source a request should read.

    v2 requires both the rollout flag and that v2 actually has data covering the requested
    range; a request reaching further back transparently falls back to v1, which has the
    full history.
    """
    if not get_option(CO_OCCURRING_ATTRS_V2_OPTION, False):
        return V1
    return V2 if _v2_covers_request_window(request) else V1
