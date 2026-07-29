"""Behaviour specific to the v2 co-occurring-attributes storage.

The shared behaviour (ordering, filtering, substring match, pagination) is covered against
both storages by the parameterized suite in
``test_endpoint_trace_item_attribute_names.py``. This module covers what only v2 can do:
per-type attribute keys (int and the four array types) and item-level counts summed from
the ``count`` column.
"""

import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.data_source.simple import Storage as StorageDataSource
from snuba.query.expressions import Column, FunctionCall
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
    get_co_occurring_attributes,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import (
    CO_OCCURRING_ATTRS_STORAGE_KEY,
    CO_OCCURRING_ATTRS_V2_OPTION,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
    CO_OCCURRING_ATTRS_V2_STORAGE_KEY,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)

# Number of items written, which is also the expected `count` for every attribute below
# since each attribute is present on every item.
NUM_ITEMS = 3

# One attribute per type, all sharing the "probe_" prefix so a substring match isolates
# them from the default attributes gen_item_message adds.
PROBE_ATTRIBUTES = {
    "probe_str": AnyValue(string_value="s"),
    "probe_int": AnyValue(int_value=3),
    "probe_float": AnyValue(double_value=1.5),
    "probe_bool": AnyValue(bool_value=True),
    "probe_arr_str": AnyValue(array_value=ArrayValue(values=[AnyValue(string_value="a")])),
    "probe_arr_int": AnyValue(array_value=ArrayValue(values=[AnyValue(int_value=1)])),
    "probe_arr_float": AnyValue(array_value=ArrayValue(values=[AnyValue(double_value=1.0)])),
    "probe_arr_bool": AnyValue(array_value=ArrayValue(values=[AnyValue(bool_value=True)])),
}


def _truncate_co_occurring_tables() -> None:
    """Empty both co-occurring-attributes tables.

    The shared ClickHouse teardown only truncates writable storages, and these tables are
    written by a materialized view rather than a consumer, so rows accumulate across tests
    in the session. The count assertions below are exact, so clear them first.
    """
    for storage_key in (CO_OCCURRING_ATTRS_STORAGE_KEY, CO_OCCURRING_ATTRS_V2_STORAGE_KEY):
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        database = cluster.get_database()
        schema = storage.get_schema()
        assert isinstance(schema, TableSchema)
        table = schema.get_local_table_name()
        for node in [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]:
            connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            connection.execute(f"TRUNCATE TABLE IF EXISTS {database}.{table}")


@pytest.fixture(autouse=True)
def setup_teardown(eap: None, redis_db: None) -> Generator[None]:
    _truncate_co_occurring_tables()
    items_storage = get_writable_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(
        items_storage,
        [
            gen_item_message(start_timestamp=BASE_TIME, attributes=dict(PROBE_ATTRIBUTES))
            for _ in range(NUM_ITEMS)
        ],
    )
    # Pin the v2 start timestamp back so the date gate does not send these requests to v1;
    # the gate itself is covered by TestCoOccurringV2DateGate below.
    with override_options(
        "snuba",
        {
            CO_OCCURRING_ATTRS_V2_OPTION: True,
            CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: 0,
        },
    ):
        yield


def _request(
    attr_type: AttributeKey.Type.ValueType,
    *,
    order_by_count: bool = False,
) -> TraceItemAttributeNamesRequest:
    order_by = (
        TraceItemAttributeNamesRequest.OrderBy(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT,
            descending=True,
        )
        if order_by_count
        else None
    )
    return TraceItemAttributeNamesRequest(
        meta=RequestMeta(
            project_ids=[1],
            organization_id=1,
            cogs_category="something",
            referrer="something",
            request_id=str(uuid.uuid4()),
            start_timestamp=Timestamp(seconds=int((BASE_TIME - timedelta(days=1)).timestamp())),
            end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(days=1)).timestamp())),
        ),
        limit=1000,
        type=attr_type,
        value_substring_match="probe_",
        order_by=order_by,
    )


def _queried_storage_key(request: TraceItemAttributeNamesRequest) -> StorageKey:
    from_clause = get_co_occurring_attributes(request).query.get_from_clause()
    assert isinstance(from_clause, StorageDataSource)
    return from_clause.key


def _names_and_types(
    attr_type: AttributeKey.Type.ValueType, *, order_by_count: bool = False
) -> list[tuple[str, str]]:
    res = EndpointTraceItemAttributeNames().execute(
        _request(attr_type, order_by_count=order_by_count)
    )
    return [(attr.name, AttributeKey.Type.Name(attr.type)) for attr in res.attributes]


@pytest.mark.eap
@pytest.mark.redis_db
class TestTraceItemAttributeNamesV2(BaseApiTest):
    def test_reads_v2_storage_when_enabled(self) -> None:
        assert (
            _queried_storage_key(_request(AttributeKey.Type.TYPE_STRING))
            == CO_OCCURRING_ATTRS_V2_STORAGE_KEY
        )

    def test_reads_v1_storage_when_disabled(self) -> None:
        """The option is a rollback switch: turning it off restores the v1 read."""
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert (
                _queried_storage_key(_request(AttributeKey.Type.TYPE_STRING))
                == CO_OCCURRING_ATTRS_STORAGE_KEY
            )

    def test_int_keys_typed_as_int(self) -> None:
        """v2 has a dedicated attributes_int key array, so an int request returns only the
        int attribute, typed TYPE_INT (v1 folds int keys into the float array)."""
        assert _names_and_types(AttributeKey.Type.TYPE_INT) == [("probe_int", "TYPE_INT")]

    def test_double_request_still_includes_int_keys(self) -> None:
        """Int attributes are double-written to a float bucket on eap_items, so they remain
        visible (as TYPE_DOUBLE) to a float/double request, as before."""
        assert _names_and_types(AttributeKey.Type.TYPE_DOUBLE) == [
            ("probe_float", "TYPE_DOUBLE"),
            ("probe_int", "TYPE_DOUBLE"),
        ]

    def test_float_request_keeps_type_float_alias(self) -> None:
        """TYPE_FLOAT is a backwards-compatible alias for TYPE_DOUBLE: same keys, but the
        response echoes the requested type."""
        assert _names_and_types(AttributeKey.Type.TYPE_FLOAT) == [
            ("probe_float", "TYPE_FLOAT"),
            ("probe_int", "TYPE_FLOAT"),
        ]

    @pytest.mark.parametrize(
        ("requested_type", "expected"),
        [
            (AttributeKey.Type.TYPE_ARRAY_STRING, ("probe_arr_str", "TYPE_ARRAY_STRING")),
            (AttributeKey.Type.TYPE_ARRAY_INT, ("probe_arr_int", "TYPE_ARRAY_INT")),
            (AttributeKey.Type.TYPE_ARRAY_DOUBLE, ("probe_arr_float", "TYPE_ARRAY_DOUBLE")),
            (AttributeKey.Type.TYPE_ARRAY_BOOL, ("probe_arr_bool", "TYPE_ARRAY_BOOL")),
        ],
    )
    def test_element_typed_array_keys(
        self,
        requested_type: AttributeKey.Type.ValueType,
        expected: tuple[str, str],
    ) -> None:
        """Each element-typed array request reads exactly its own key array."""
        assert _names_and_types(requested_type) == [expected]

    def test_untyped_array_returns_all_four_element_types(self) -> None:
        """The deprecated untyped TYPE_ARRAY has no element type, so it surfaces the keys of
        all four array columns, each tagged with its own type."""
        assert _names_and_types(AttributeKey.Type.TYPE_ARRAY) == [
            ("probe_arr_bool", "TYPE_ARRAY_BOOL"),
            ("probe_arr_float", "TYPE_ARRAY_DOUBLE"),
            ("probe_arr_int", "TYPE_ARRAY_INT"),
            ("probe_arr_str", "TYPE_ARRAY_STRING"),
        ]

    def test_unspecified_type_includes_array_keys_without_duplicating_ints(self) -> None:
        """TYPE_UNSPECIFIED surfaces every type. Int keys appear once (as TYPE_DOUBLE, via
        the float bucket they are double-written to) rather than twice, and the array-typed
        keys — invisible on v1 — are included."""
        assert _names_and_types(AttributeKey.Type.TYPE_UNSPECIFIED) == [
            ("probe_arr_bool", "TYPE_ARRAY_BOOL"),
            ("probe_arr_float", "TYPE_ARRAY_DOUBLE"),
            ("probe_arr_int", "TYPE_ARRAY_INT"),
            ("probe_arr_str", "TYPE_ARRAY_STRING"),
            ("probe_bool", "TYPE_BOOLEAN"),
            ("probe_float", "TYPE_DOUBLE"),
            ("probe_int", "TYPE_DOUBLE"),
            ("probe_str", "TYPE_STRING"),
        ]

    def test_count_sums_occurrence_column(self) -> None:
        """v2 rows carry an occurrence `count`, so count ordering sums that column and
        reports how many items the key was seen on. On v1 the same request counts rows
        (one per distinct attribute set), which here would be 1 rather than NUM_ITEMS.
        """
        res = EndpointTraceItemAttributeNames().execute(
            _request(AttributeKey.Type.TYPE_STRING, order_by_count=True)
        )
        counts = {attr.name: attr.count for attr in res.attributes}
        assert counts == {"probe_str": NUM_ITEMS}

        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            v1_res = EndpointTraceItemAttributeNames().execute(
                _request(AttributeKey.Type.TYPE_STRING, order_by_count=True)
            )
        v1_counts = {attr.name: attr.count for attr in v1_res.attributes}
        assert v1_counts == {"probe_str": 1}

    def test_count_populated_for_int_and_array_keys(self) -> None:
        """The summed count is available for the types v1 could not surface at all."""
        for attr_type in (
            AttributeKey.Type.TYPE_INT,
            AttributeKey.Type.TYPE_ARRAY_STRING,
            AttributeKey.Type.TYPE_ARRAY_BOOL,
        ):
            res = EndpointTraceItemAttributeNames().execute(
                _request(attr_type, order_by_count=True)
            )
            assert [attr.count for attr in res.attributes] == [NUM_ITEMS], (
                f"unexpected counts for {AttributeKey.Type.Name(attr_type)}"
            )

    def test_substring_match_prefilters_the_typed_array(self) -> None:
        """The arrayExists row-prefilter must target the key arrays the request actually
        reads, otherwise a typed request would be filtered against the wrong column."""
        query = get_co_occurring_attributes(_request(AttributeKey.Type.TYPE_ARRAY_INT)).query
        condition = query.get_condition()
        assert condition is not None
        array_exists_columns = {
            param.column_name
            for exp in condition
            if isinstance(exp, FunctionCall) and exp.function_name == "arrayExists"
            for param in exp.parameters
            if isinstance(param, Column)
        }
        assert array_exists_columns == {"attributes_array_int"}


@pytest.mark.eap
@pytest.mark.redis_db
class TestCoOccurringV2DateGate(BaseApiTest):
    """The v2 tables only hold data from when their materialized view was created, so a
    request reaching back before that must read v1 even with the rollout flag on. Otherwise
    attributes that only existed in the earlier part of the range vanish from the results.
    """

    # The default cutoff, 2026-07-27 00:00 UTC, is a Monday, matching the weekly toMonday()
    # bucketing of the `date` column.
    V2_START = datetime.fromtimestamp(CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT, UTC)

    @pytest.fixture(autouse=True)
    def use_real_cutoff(self) -> Generator[None]:
        """Undo the module fixture's pinned-to-0 cutoff so the gate is actually exercised."""
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_OPTION: True,
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: (
                    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT
                ),
            },
        ):
            yield

    def _storage_for_start(self, start: datetime) -> StorageKey:
        req = _request(AttributeKey.Type.TYPE_STRING)
        req.meta.start_timestamp.FromDatetime(start)
        req.meta.end_timestamp.FromDatetime(start + timedelta(hours=1))
        return _queried_storage_key(req)

    def test_default_cutoff_is_a_monday(self) -> None:
        """The cutoff must land on a Monday. `date` is bucketed with toMonday() and the query
        rounds its lower bound down to the previous Monday, so a mid-week cutoff would let a
        request starting later in that week round *below* it and read a bucket only v1 has."""
        assert self.V2_START.weekday() == 0
        assert (self.V2_START.hour, self.V2_START.minute, self.V2_START.second) == (0, 0, 0)

    def test_request_starting_at_the_cutoff_reads_v2(self) -> None:
        assert self._storage_for_start(self.V2_START) == CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def test_request_starting_after_the_cutoff_reads_v2(self) -> None:
        """Later in the same week still rounds down to exactly the cutoff bucket."""
        assert (
            self._storage_for_start(self.V2_START + timedelta(days=3))
            == CO_OCCURRING_ATTRS_V2_STORAGE_KEY
        )

    def test_request_starting_before_the_cutoff_falls_back_to_v1(self) -> None:
        """One second earlier rounds to the previous Monday, which v2 never populated."""
        assert (
            self._storage_for_start(self.V2_START - timedelta(seconds=1))
            == CO_OCCURRING_ATTRS_STORAGE_KEY
        )

    def test_request_reaching_far_back_falls_back_to_v1(self) -> None:
        assert (
            self._storage_for_start(self.V2_START - timedelta(days=30))
            == CO_OCCURRING_ATTRS_STORAGE_KEY
        )

    def test_gate_uses_rounded_lower_bound_not_raw_timestamp(self) -> None:
        """Regression guard: the gate must compare the bucket the query actually reads.

        A request starting mid-week just *after* the cutoff reads from the Monday before it.
        If the cutoff were mid-week (here, the Wednesday the tables were really created) a
        raw-timestamp comparison would admit such a request even though it reads the
        preceding bucket, which only exists in v1.
        """
        wednesday_cutoff = self.V2_START + timedelta(days=2)
        with override_options(
            "snuba",
            {
                CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: int(wednesday_cutoff.timestamp()),
            },
        ):
            # Starts after the cutoff instant, but rounds down to the Monday before it.
            assert (
                self._storage_for_start(wednesday_cutoff + timedelta(hours=1))
                == CO_OCCURRING_ATTRS_STORAGE_KEY
            )

    def test_start_timestamp_is_configurable(self) -> None:
        """Lowering the option (e.g. after a backfill) widens the v2 window."""
        before_cutoff = self.V2_START - timedelta(days=30)
        assert self._storage_for_start(before_cutoff) == CO_OCCURRING_ATTRS_STORAGE_KEY
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: 0}):
            assert self._storage_for_start(before_cutoff) == CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def test_flag_off_reads_v1_even_inside_the_v2_window(self) -> None:
        """The rollout flag remains an unconditional off switch."""
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert self._storage_for_start(self.V2_START) == CO_OCCURRING_ATTRS_STORAGE_KEY

    def test_gated_fallback_still_returns_attributes(self) -> None:
        """A request spanning the cutoff is served by v1, so it must still return the
        attributes rather than an empty result."""
        req = _request(AttributeKey.Type.TYPE_STRING)
        req.meta.start_timestamp.FromDatetime(self.V2_START - timedelta(days=30))
        assert _queried_storage_key(req) == CO_OCCURRING_ATTRS_STORAGE_KEY
        res = EndpointTraceItemAttributeNames().execute(req)
        assert [attr.name for attr in res.attributes] == ["probe_str"]
