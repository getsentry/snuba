import pytest

from snuba import state
from snuba.query.expressions import Column, DangerousRawSQL, FunctionCall
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    local_join_clickhouse_settings,
    trace_id_in_subquery_condition,
    use_local_join_for_cross_item_queries,
)

_SUBQUERY = "SELECT trace_id FROM eap_items_1_dist WHERE 1"


@pytest.mark.redis_db
class TestCrossItemLocalJoinHelpers:
    """Unit tests for the cross-item local-join helpers (EAP-377). These exercise
    the pure query-building/gating logic without touching ClickHouse."""

    def test_disabled_by_default(self) -> None:
        assert use_local_join_for_cross_item_queries() is False
        assert local_join_clickhouse_settings() == {}

    def test_enabled(self) -> None:
        state.set_config("use_local_join_for_cross_item_queries", 1)
        assert use_local_join_for_cross_item_queries() is True
        assert local_join_clickhouse_settings() == {"distributed_product_mode": "local"}

    def test_condition_default_path(self) -> None:
        # When the optimization is off, we emit `in(trace_id, (<subquery>))`. The
        # bare trace_id column is later dash-stripped by the UUIDColumnProcessor.
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        assert isinstance(cond, FunctionCall)
        assert cond.function_name == "in"
        lhs, rhs = cond.parameters
        assert isinstance(lhs, Column)
        assert lhs.column_name == "trace_id"
        assert isinstance(rhs, DangerousRawSQL)
        assert rhs.sql == f"({_SUBQUERY})"

    def test_condition_local_join_path(self) -> None:
        # When the optimization is on, the whole condition is raw SQL on the bare
        # `trace_id` UUID column so (a) the bloom-filter index applies and (b) it
        # stays in WHERE (no trace_id Column node for PrewhereProcessor to move).
        state.set_config("use_local_join_for_cross_item_queries", 1)
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        assert isinstance(cond, DangerousRawSQL)
        assert cond.sql == f"trace_id IN ({_SUBQUERY})"
        assert not any(isinstance(e, Column) for e in cond)
