"""Behaviour only v2 can provide: per-type attribute keys, summed item counts, last_seen.

Behaviour shared with v1 is covered against both storages by the parameterized suite in
``test_endpoint_trace_item_attribute_names.py``.
"""

import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options import OptionValue
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_attributes_pb2 import (
    TraceItemAttributeNamesRequest,
    TraceItemAttributeNamesResponse,
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
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from snuba.web.rpc.v1.endpoint_trace_item_attribute_names import (
    EndpointTraceItemAttributeNames,
    get_co_occurring_attributes,
)
from snuba.web.rpc.v1.resolvers.R_eap_items import co_occurring_attrs
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import (
    CO_OCCURRING_ATTRS_STORAGE_KEY,
    CO_OCCURRING_ATTRS_V2_OPTION,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT,
    CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION,
    CO_OCCURRING_ATTRS_V2_STORAGE_KEY,
    V1,
    V2,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)

# The two ways a request ends up reading v1: the rollout flag is off, or the flag is on but
# the date gate routed an older time range there. Behaviour must be identical in both.
ROUTES_TO_V1: list[dict[str, OptionValue]] = [
    {CO_OCCURRING_ATTRS_V2_OPTION: False},
    {
        CO_OCCURRING_ATTRS_V2_OPTION: True,
        CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: int(
            (BASE_TIME + timedelta(days=365)).timestamp()
        ),
    },
]

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
    """The shared teardown only truncates writable storages, and these are materialized-view
    targets, so rows otherwise accumulate across the session. Count assertions here are exact.
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
    # Pin the start timestamp back so the date gate (covered separately below) stays out of
    # the way.
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
    order_by_column: TraceItemAttributeNamesRequest.OrderBy.Column.ValueType | None = None,
    descending: bool = True,
) -> TraceItemAttributeNamesRequest:
    if order_by_column is None and order_by_count:
        order_by_column = TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT
    order_by = (
        TraceItemAttributeNamesRequest.OrderBy(column=order_by_column, descending=descending)
        if order_by_column is not None
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
        """Dedicated attributes_int array, so int keys keep their type (v1 folds them into
        the float array)."""
        assert _names_and_types(AttributeKey.Type.TYPE_INT) == [("probe_int", "TYPE_INT")]

    def test_double_request_still_includes_int_keys(self) -> None:
        """Int attributes are double-written to a float bucket, so they stay visible (as
        TYPE_DOUBLE) to a double request."""
        assert _names_and_types(AttributeKey.Type.TYPE_DOUBLE) == [
            ("probe_float", "TYPE_DOUBLE"),
            ("probe_int", "TYPE_DOUBLE"),
        ]

    def test_float_request_keeps_type_float_alias(self) -> None:
        """TYPE_FLOAT is an alias for TYPE_DOUBLE: same keys, requested type echoed back."""
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
        """Untyped TYPE_ARRAY has no element type, so it surfaces all four array columns."""
        assert _names_and_types(AttributeKey.Type.TYPE_ARRAY) == [
            ("probe_arr_bool", "TYPE_ARRAY_BOOL"),
            ("probe_arr_float", "TYPE_ARRAY_DOUBLE"),
            ("probe_arr_int", "TYPE_ARRAY_INT"),
            ("probe_arr_str", "TYPE_ARRAY_STRING"),
        ]

    def test_unspecified_type_includes_array_keys_without_duplicating_ints(self) -> None:
        """Every type, with int keys appearing once (as TYPE_DOUBLE, via the float bucket)
        rather than twice, and the array-typed keys v1 cannot see."""
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
        """Rows carry an occurrence `count`, so this sums to the number of items. The same
        request on v1 counts attribute sets, which here is 1."""
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
        """Available for the types v1 cannot surface at all."""
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
        """The prefilter must target the arrays actually read, not some other column."""
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
    """A request reaching back before v2's materialized view existed must read v1, or the
    attributes that only existed in the earlier part of its range vanish.
    """

    V2_START = datetime.fromtimestamp(CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_DEFAULT, UTC)

    @pytest.fixture(autouse=True)
    def use_real_cutoff(self) -> Generator[None]:
        """Undo the module fixture's pinned cutoff so the gate is exercised."""
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
        """`date` is bucketed with toMonday() and queries round down to the previous Monday, so
        a mid-week cutoff would let a request round *below* it into a v1-only bucket."""
        assert self.V2_START.weekday() == 0
        assert (self.V2_START.hour, self.V2_START.minute, self.V2_START.second) == (0, 0, 0)

    def test_request_starting_at_the_cutoff_reads_v2(self) -> None:
        assert self._storage_for_start(self.V2_START) == CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def test_request_starting_after_the_cutoff_reads_v2(self) -> None:
        """Later in the same week rounds down to exactly the cutoff bucket."""
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
        """The gate must compare the bucket the query reads, not the raw start timestamp.

        With a mid-week cutoff, a request starting just after it still reads from the Monday
        before — a bucket only v1 has — so a raw-timestamp comparison would wrongly admit it.
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
        """Lowering the option widens the v2 window."""
        before_cutoff = self.V2_START - timedelta(days=30)
        assert self._storage_for_start(before_cutoff) == CO_OCCURRING_ATTRS_STORAGE_KEY
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_START_TIMESTAMP_OPTION: 0}):
            assert self._storage_for_start(before_cutoff) == CO_OCCURRING_ATTRS_V2_STORAGE_KEY

    def test_flag_off_reads_v1_even_inside_the_v2_window(self) -> None:
        """The rollout flag remains an unconditional off switch."""
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            assert self._storage_for_start(self.V2_START) == CO_OCCURRING_ATTRS_STORAGE_KEY

    def test_gated_fallback_still_returns_attributes(self) -> None:
        """Served by v1, so it must still return attributes rather than an empty result."""
        req = _request(AttributeKey.Type.TYPE_STRING)
        req.meta.start_timestamp.FromDatetime(self.V2_START - timedelta(days=30))
        assert _queried_storage_key(req) == CO_OCCURRING_ATTRS_STORAGE_KEY
        res = EndpointTraceItemAttributeNames().execute(req)
        assert [attr.name for attr in res.attributes] == ["probe_str"]


@pytest.mark.eap
@pytest.mark.redis_db
class TestLastSeen(BaseApiTest):
    """Reporting and ordering by `last_seen`, which only v2 records.

    The module fixture writes every probe attribute at one timestamp, which cannot tell a
    recency ordering from an arbitrary one, so this class writes its own at distinct times.
    """

    # Recency, frequency and name each give a *different* order over this data, so a test
    # asserting one of them cannot be satisfied by accident by another. In particular the
    # oldest attribute is the most frequent, so recency and count orderings are opposites.
    #
    #   name        hours ago   items written   recency rank   count rank
    #   ls_oldest       4             3              last        first
    #   ls_middle       2             2              middle      middle
    #   ls_newest       0             1              first        last
    STAGGERED = ["ls_oldest", "ls_middle", "ls_newest"]
    OFFSETS = {"ls_oldest": 4, "ls_middle": 2, "ls_newest": 0}
    ITEM_COUNTS = {"ls_oldest": 3, "ls_middle": 2, "ls_newest": 1}

    # Most recent first: the reverse of write order.
    BY_RECENCY_DESC = ["ls_newest", "ls_middle", "ls_oldest"]
    # Most frequent first: the exact opposite of BY_RECENCY_DESC, which is what makes a
    # degraded recency request distinguishable from an honoured one. Both storages agree on
    # this order here (see the filler attribute below), though they count different things.
    BY_COUNT_DESC = ["ls_oldest", "ls_middle", "ls_newest"]

    @pytest.fixture(autouse=True)
    def staggered_items(self) -> None:
        items_storage = get_writable_storage(StorageKey("eap_items"))
        for name, hours_ago in self.OFFSETS.items():
            # All of an attribute's items share a timestamp, so last_seen stays exact while
            # the number of items drives its count.
            #
            # Each item also gets a unique filler attribute so that every item is a distinct
            # attribute-key *set*. Without it the items for one attribute collapse into a
            # single set, and v1 — which counts sets, not items — reports 1 for everything,
            # making its count ordering degenerate into the name tiebreak. The filler names
            # deliberately avoid the "ls_" substring the tests filter on, so they stay out of
            # the results.
            write_raw_unprocessed_events(
                items_storage,
                [
                    gen_item_message(
                        start_timestamp=BASE_TIME - timedelta(hours=hours_ago),
                        attributes={
                            name: AnyValue(string_value="x"),
                            f"pad{i}_{name[3:]}": AnyValue(string_value="x"),
                        },
                    )
                    for i in range(self.ITEM_COUNTS[name])
                ],
            )

    def _run(
        self,
        *,
        column: TraceItemAttributeNamesRequest.OrderBy.Column.ValueType,
        descending: bool = True,
    ) -> list[TraceItemAttributeNamesResponse.Attribute]:
        req = _request(AttributeKey.Type.TYPE_STRING, order_by_column=column, descending=descending)
        req.value_substring_match = "ls_"
        return list(EndpointTraceItemAttributeNames().execute(req).attributes)

    def test_orders_by_recency_descending(self) -> None:
        """Most recently used attributes first."""
        attributes = self._run(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
        )
        assert [a.name for a in attributes] == self.BY_RECENCY_DESC

    def test_orders_by_recency_ascending(self) -> None:
        attributes = self._run(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN,
            descending=False,
        )
        assert [a.name for a in attributes] == list(reversed(self.BY_RECENCY_DESC))

    def test_last_seen_values_reflect_when_each_was_written(self) -> None:
        """The reported timestamps must be the real ones, not just correctly ordered."""
        attributes = self._run(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
        )
        seen = {a.name: a.last_seen.ToDatetime() for a in attributes}
        assert set(seen) == set(self.STAGGERED)
        for name, hours_ago in self.OFFSETS.items():
            expected = (BASE_TIME - timedelta(hours=hours_ago)).replace(tzinfo=None)
            # The roll-up buckets by item timestamp, so this is exact rather than approximate.
            assert seen[name] == expected, f"{name} last_seen {seen[name]} != {expected}"

    def test_last_seen_is_populated_under_count_ordering_too(self) -> None:
        """Selected whenever the storage has it, so ordering by frequency still reports it."""
        attributes = self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT)
        assert attributes
        for attribute in attributes:
            assert attribute.HasField("last_seen"), f"{attribute.name} has no last_seen"

    def test_last_seen_is_unset_under_name_ordering(self) -> None:
        """Name ordering does not aggregate, so the field stays unset rather than zeroed."""
        attributes = self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_NAME)
        assert attributes
        for attribute in attributes:
            assert not attribute.HasField("last_seen")

    @pytest.mark.parametrize("route_index", [0, 1], ids=["flag_off", "date_gate"])
    def test_recency_ordering_degrades_to_count_on_v1(self, route_index: int) -> None:
        """v1 has no last_seen, so a recency request falls back to frequency ordering rather
        than failing, and stays detectable because last_seen is absent from the response.

        Covers both ways a request lands on v1: the flag being off, and the date gate.
        """
        with override_options("snuba", ROUTES_TO_V1[route_index]):
            attributes = self._run(
                column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
            )
        # No error, and the attributes still come back — in count order, which over this
        # data is the exact reverse of the recency order that was asked for.
        assert [a.name for a in attributes] == self.BY_COUNT_DESC
        # Counts are populated; last_seen cannot be, so it stays unset.
        assert all(a.HasField("count") for a in attributes)
        assert all(not a.HasField("last_seen") for a in attributes)

    @pytest.mark.parametrize("route_index", [0, 1], ids=["flag_off", "date_gate"])
    def test_degraded_ordering_matches_a_real_count_ordering(self, route_index: int) -> None:
        """Catches the ordering being derived inconsistently between the ClickHouse ORDER BY
        and the Python re-sort, which would make the two drift apart."""
        with override_options("snuba", ROUTES_TO_V1[route_index]):
            degraded = self._run(
                column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
            )
            by_count = self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT)
        assert [a.name for a in degraded] == [a.name for a in by_count]
        assert [a.count for a in degraded] == [a.count for a in by_count]

    def test_other_orderings_still_work_on_v1(self) -> None:
        """Only recency ordering degrades."""
        with override_options("snuba", {CO_OCCURRING_ATTRS_V2_OPTION: False}):
            by_count = self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT)
            assert [a.name for a in by_count] == self.BY_COUNT_DESC
            # v1 cannot report last_seen, so it must be left unset rather than zeroed.
            assert all(not a.HasField("last_seen") for a in by_count)

    def test_recency_ordering_is_honoured_on_v2(self) -> None:
        """Guard against the degrade firing when it should not."""
        by_recency = self._run(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
        )
        assert [a.name for a in by_recency] == self.BY_RECENCY_DESC
        assert all(a.HasField("last_seen") for a in by_recency)

    @pytest.mark.parametrize("route_index", [0, 1], ids=["flag_off", "date_gate"])
    def test_degrade_is_recorded_as_a_metric(self, route_index: int) -> None:
        """Invisible to the caller, so it has to be visible to us during the rollout."""
        with override_options("snuba", ROUTES_TO_V1[route_index]):
            self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN)
        calls = get_recorded_metric_calls("increment", "rpc.attribute_names_order_by_degraded")
        assert calls, "expected a degrade metric"
        tags = calls[-1].tags or {}
        assert tags.get("requested") == "COLUMN_LAST_SEEN"
        assert tags.get("applied") == "COLUMN_COUNT"
        assert tags.get("storage") == CO_OCCURRING_ATTRS_STORAGE_KEY.value

    def test_no_degrade_metric_when_honoured(self) -> None:
        """Guard against the metric firing on v2."""
        self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN)
        assert not get_recorded_metric_calls("increment", "rpc.attribute_names_order_by_degraded")

    def test_recency_and_count_orderings_differ_on_v2(self) -> None:
        """Checks the fixture data as much as the code: the two aggregating orderings are exact
        opposites here, so neither can be satisfied by accident by the other."""
        by_recency = self._run(
            column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
        )
        by_count = self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_COUNT)
        assert [a.name for a in by_recency] == self.BY_RECENCY_DESC
        assert [a.name for a in by_count] == self.BY_COUNT_DESC
        assert list(reversed(self.BY_COUNT_DESC)) == self.BY_RECENCY_DESC
        # v2 counts items, so the counts are the number written rather than a flat 1.
        assert {a.name: a.count for a in by_count} == self.ITEM_COUNTS

    def test_source_is_resolved_once_per_request(self) -> None:
        """Resolving reads runtime options, so a second resolution could see a different value
        if an option flips mid-request: the query would be built for one storage while the
        response is re-sorted as if it were the other (regression guard: the count was 2).
        """
        with mock.patch.object(
            co_occurring_attrs, "for_request", wraps=co_occurring_attrs.for_request
        ) as resolve:
            self._run(column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN)
        assert resolve.call_count == 1, (
            f"source resolved {resolve.call_count} times; the query and the response "
            "converter must share a single resolution"
        )

    def test_ordering_survives_the_source_changing_mid_request(self) -> None:
        """Simulates an option flipping between resolutions by returning v2 then v1. With a
        single resolution the second value is never read, so the result stays coherent.
        """
        sources = iter([V2, V1])

        def flipping(_request: TraceItemAttributeNamesRequest) -> object:
            return next(sources, V1)

        with mock.patch.object(co_occurring_attrs, "for_request", side_effect=flipping):
            attributes = self._run(
                column=TraceItemAttributeNamesRequest.OrderBy.Column.COLUMN_LAST_SEEN
            )
        # v2 was resolved, so recency ordering is honoured end to end: the order is the
        # requested one and last_seen is populated, not the misordered mix the double
        # resolution produced.
        assert [a.name for a in attributes] == self.BY_RECENCY_DESC
        assert all(a.HasField("last_seen") for a in attributes)
