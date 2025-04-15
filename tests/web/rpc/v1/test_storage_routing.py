from unittest.mock import MagicMock

from snuba.downsampled_storage_tiers import Tier
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    ClickhouseQuerySettings,
    RoutedRequestType,
    RoutingContext,
)


class RoutingStrategyFailsToSelectTier(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        raise Exception

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategySelectsTier8(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


class RoutingStrategyUpdatesQuerySettings(BaseRoutingStrategy):
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        return Tier.TIER_8, {"some_setting": "some_value"}

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        return QueryResult(result=MagicMock(), extra=MagicMock())

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        pass


def test_target_tier_is_tier_1_if_routing_strategy_fails_to_decide_tier() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=HTTPQuerySettings(),
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategyFailsToSelectTier().run_query_to_correct_tier(routing_context)
    assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_1


def test_target_tier_is_set_in_routing_context() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=HTTPQuerySettings(),
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategySelectsTier8().run_query_to_correct_tier(routing_context)
    assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_8


def test_merge_query_settings() -> None:
    routing_context = RoutingContext(
        in_msg=MagicMock(spec=RoutedRequestType),
        timer=MagicMock(spec=Timer),
        build_query=MagicMock(),
        query_settings=HTTPQuerySettings(),
        query_result=MagicMock(spec=QueryResult),
        extra_info={},
    )
    RoutingStrategyUpdatesQuerySettings().run_query_to_correct_tier(routing_context)
    assert routing_context.query_settings.get_sampling_tier() == Tier.TIER_8
    assert routing_context.query_settings.get_clickhouse_settings() == {
        "some_setting": "some_value"
    }
