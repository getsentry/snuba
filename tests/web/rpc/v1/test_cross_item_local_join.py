from snuba.query.expressions import Column, DangerousRawSQL
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    LOCAL_JOIN_CLICKHOUSE_SETTINGS,
    trace_id_in_subquery_condition,
)

_SUBQUERY = "SELECT trace_id FROM eap_items_1_dist WHERE 1"


class TestCrossItemLocalJoinHelpers:
    """Unit tests for the cross-item local-join helpers (EAP-377). These exercise
    the pure query-building logic without touching ClickHouse."""

    def test_local_join_clickhouse_settings(self) -> None:
        assert LOCAL_JOIN_CLICKHOUSE_SETTINGS == {"distributed_product_mode": "local"}

    def test_trace_id_in_subquery_condition(self) -> None:
        # The whole condition is raw SQL on the bare `trace_id` UUID column so
        # (a) the bloom-filter index applies (no replaceAll(toString(...)) wrapping)
        # and (b) it stays in WHERE (no trace_id Column node for PrewhereProcessor
        # to move to PREWHERE).
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        assert isinstance(cond, DangerousRawSQL)
        assert cond.sql == f"trace_id IN ({_SUBQUERY})"
        assert not any(isinstance(e, Column) for e in cond)
