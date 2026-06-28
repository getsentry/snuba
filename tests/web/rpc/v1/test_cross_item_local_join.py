import pytest

from snuba import state
from snuba.downsampled_storage_tiers import Tier
from snuba.query.expressions import Column, DangerousRawSQL, FunctionCall
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    CROSS_ITEM_DISTRIBUTED_PRODUCT_MODE,
    apply_cross_item_outer_query_settings,
    trace_id_in_subquery_condition,
)

_SUBQUERY = "SELECT trace_id FROM eap_items_1_dist WHERE 1"


@pytest.mark.redis_db
class TestCrossItemLocalJoinHelpers:
    """Unit tests for the cross-item local-join helpers, which are gated behind the
    ``use_local_join_for_cross_item_queries`` runtime config (EAP-377)."""

    def test_condition_local_join_enabled(self) -> None:
        state.set_config("use_local_join_for_cross_item_queries", 1)
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        # Raw SQL on the bare trace_id UUID: bloom-filter usable, no Column node so it
        # stays in WHERE rather than being moved to PREWHERE.
        assert isinstance(cond, DangerousRawSQL)
        assert cond.sql == f"trace_id IN ({_SUBQUERY})"
        assert not any(isinstance(e, Column) for e in cond)

    def test_condition_local_join_disabled(self) -> None:
        state.set_config("use_local_join_for_cross_item_queries", 0)
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        # Legacy: in(trace_id, (<subquery>)); trace_id is later dash-stripped.
        assert isinstance(cond, FunctionCall)
        assert cond.function_name == "in"
        lhs, rhs = cond.parameters
        assert isinstance(lhs, Column)
        assert lhs.column_name == "trace_id"
        assert isinstance(rhs, DangerousRawSQL)
        assert rhs.sql == f"({_SUBQUERY})"

    def test_settings_local_join_enabled(self) -> None:
        state.set_config("use_local_join_for_cross_item_queries", 1)
        query_settings = HTTPQuerySettings()
        apply_cross_item_outer_query_settings(
            query_settings, has_trace_filters=True, sampling_tier=Tier.TIER_1
        )
        assert (
            query_settings.get_clickhouse_settings().get("distributed_product_mode")
            == CROSS_ITEM_DISTRIBUTED_PRODUCT_MODE
        )

    def test_settings_local_join_disabled(self) -> None:
        state.set_config("use_local_join_for_cross_item_queries", 0)
        query_settings = HTTPQuerySettings()
        apply_cross_item_outer_query_settings(
            query_settings, has_trace_filters=True, sampling_tier=Tier.TIER_1
        )
        assert "distributed_product_mode" not in query_settings.get_clickhouse_settings()

    def test_settings_non_cross_item_applies_sampling(self) -> None:
        query_settings = HTTPQuerySettings()
        apply_cross_item_outer_query_settings(
            query_settings, has_trace_filters=False, sampling_tier=Tier.TIER_1
        )
        assert "distributed_product_mode" not in query_settings.get_clickhouse_settings()
        # Non-cross-item queries still get their sampling tier applied.
        assert query_settings.get_sampling_tier() == Tier.TIER_1
