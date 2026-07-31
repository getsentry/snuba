from sentry_options.testing import override_options

from snuba.downsampled_storage_tiers import Tier
from snuba.query.expressions import Column, DangerousRawSQL
from snuba.query.query_settings import HTTPQuerySettings
from snuba.web.rpc.v1.resolvers.common.cross_item_queries import (
    CROSS_ITEM_DISTRIBUTED_PRODUCT_MODE,
    apply_cross_item_outer_query_settings,
    trace_id_in_subquery_condition,
)

_SUBQUERY = "SELECT trace_id FROM eap_items_1_dist WHERE 1"


class TestCrossItemLocalJoinHelpers:
    """Unit tests for the cross-item local-join helpers (EAP-377)."""

    def test_trace_id_in_subquery_condition_is_bare_raw_sql(self) -> None:
        # Always emitted as raw SQL on the bare trace_id UUID so the bloom-filter index
        # applies and it stays in WHERE (no Column node for PrewhereProcessor to move).
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        assert isinstance(cond, DangerousRawSQL)
        assert cond.sql == f"trace_id IN ({_SUBQUERY})"
        assert not any(isinstance(e, Column) for e in cond)

    def test_settings_local_join_enabled(self) -> None:
        query_settings = HTTPQuerySettings()
        with override_options("snuba", {"use_local_join_for_cross_item_queries": True}):
            apply_cross_item_outer_query_settings(
                query_settings, has_trace_filters=True, sampling_tier=Tier.TIER_1
            )
        assert (
            query_settings.get_clickhouse_settings().get("distributed_product_mode")
            == CROSS_ITEM_DISTRIBUTED_PRODUCT_MODE
        )

    def test_settings_local_join_disabled(self) -> None:
        # Default (flag off): no distributed_product_mode override.
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
