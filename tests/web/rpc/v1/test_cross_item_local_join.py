import pytest

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

    def test_trace_id_in_subquery_condition(self) -> None:
        # The whole condition is raw SQL on the bare `trace_id` UUID column so
        # (a) the bloom-filter index applies (no replaceAll(toString(...)) wrapping)
        # and (b) it stays in WHERE (no trace_id Column node for PrewhereProcessor
        # to move to PREWHERE).
        cond = trace_id_in_subquery_condition(_SUBQUERY)
        assert isinstance(cond, DangerousRawSQL)
        assert cond.sql == f"trace_id IN ({_SUBQUERY})"
        assert not any(isinstance(e, Column) for e in cond)


@pytest.mark.redis_db
class TestApplyCrossItemOuterQuerySettings:
    """The shared outer-query settings helper used by all EAP resolvers (EAP-377)."""

    def test_cross_item_sets_local_join(self) -> None:
        query_settings = HTTPQuerySettings()
        apply_cross_item_outer_query_settings(
            query_settings, has_trace_filters=True, sampling_tier=Tier.TIER_1
        )
        assert (
            query_settings.get_clickhouse_settings().get("distributed_product_mode")
            == CROSS_ITEM_DISTRIBUTED_PRODUCT_MODE
        )

    def test_non_cross_item_leaves_local_join_unset(self) -> None:
        query_settings = HTTPQuerySettings()
        apply_cross_item_outer_query_settings(
            query_settings, has_trace_filters=False, sampling_tier=Tier.TIER_1
        )
        assert "distributed_product_mode" not in query_settings.get_clickhouse_settings()
        # Non-cross-item queries still get their sampling tier applied.
        assert query_settings.get_sampling_tier() == Tier.TIER_1
