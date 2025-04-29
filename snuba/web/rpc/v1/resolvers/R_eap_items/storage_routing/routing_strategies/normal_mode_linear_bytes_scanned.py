from typing import Callable, Dict, Optional, TypeAlias, Union, cast

import sentry_sdk
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_sdk.tracing import Span

from snuba import state
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    ClickhouseQuerySettings,
    RoutingContext,
)

_SAMPLING_IN_STORAGE_PREFIX = "sampling_in_storage_"
_DOWNSAMPLING_TIER_MULTIPLIERS: Dict[Tier, int] = {
    Tier.TIER_512: 1,
    Tier.TIER_64: 8,
    Tier.TIER_8: 64,
    Tier.TIER_1: 512,
}
MetricsBackendType: TypeAlias = Callable[
    [str, Union[int, float], Optional[Dict[str, str]], Optional[str]], None
]


class NormalModeLinearBytesScannedRoutingStrategy(BaseRoutingStrategy):
    def _get_time_budget_ms(self) -> float:
        sentry_timeout_ms = cast(
            int,
            state.get_int_config(self.config_key() + "_sentry_timeout", default=8000),
        )  # 8s
        error_budget_ms = cast(
            int,
            state.get_int_config(self.config_key() + "_error_budget", default=500),
        )  # 0.5s
        return sentry_timeout_ms - error_budget_ms

    def _get_most_downsampled_tier(self) -> Tier:
        return Tier.TIER_64

    def _get_multiplier(self, tier: Tier) -> int:
        return int(
            _DOWNSAMPLING_TIER_MULTIPLIERS[tier]
            / _DOWNSAMPLING_TIER_MULTIPLIERS[self._get_most_downsampled_tier()]
        )

    def _is_normal_mode(self, routing_context: RoutingContext) -> bool:
        if not routing_context.in_msg.meta.HasField("downsampled_storage_config"):
            return False
        mode = routing_context.in_msg.meta.downsampled_storage_config.mode
        return (
            mode == DownsampledStorageConfig.MODE_NORMAL
            or mode == DownsampledStorageConfig.MODE_PREFLIGHT
            or mode == DownsampledStorageConfig.MODE_BEST_EFFORT
        )

    def _get_query_bytes_scanned(
        self, res: QueryResult, span: Span | None = None
    ) -> int:
        return cast(int, res.result.get("profile", {}).get("progress_bytes", 0))  # type: ignore

    def _get_query_duration_ms(self, res: QueryResult) -> float:
        return cast(float, res.result.get("profile", {}).get("elapsed", 0) * 1000)  # type: ignore

    def _get_bytes_scanned_limit(self) -> int:
        return cast(
            int,
            state.get_int_config(
                self.config_key() + "_bytes_scanned_per_query_limit",
                default=161061273600,
            ),
        )

    def _get_target_tier(
        self,
        most_downsampled_tier_query_result: QueryResult,
        routing_context: RoutingContext,
    ) -> Tier:
        estimated_query_bytes_scanned_to_this_tier = -1
        with sentry_sdk.start_span(op="_get_target_tier") as span:
            most_downsampled_tier_query_bytes_scanned = self._get_query_bytes_scanned(
                most_downsampled_tier_query_result, span
            )

            span.set_data(
                _SAMPLING_IN_STORAGE_PREFIX + "most_downsampled_query_bytes_scanned",
                most_downsampled_tier_query_bytes_scanned,
            )

            target_tier = self._get_most_downsampled_tier()
            estimated_target_tier_bytes_scanned = (
                most_downsampled_tier_query_bytes_scanned
                * self._get_multiplier(target_tier)
            )

            for tier in sorted(Tier, reverse=True)[:-1]:
                with sentry_sdk.start_span(
                    op=f"_get_target_tier.Tier_{tier}"
                ) as tier_specific_span:
                    estimated_query_bytes_scanned_to_this_tier = (
                        most_downsampled_tier_query_bytes_scanned
                        * self._get_multiplier(tier)
                    )
                    self._record_value_in_span_and_DD(
                        routing_context,
                        self.metrics.distribution,
                        "estimated_query_bytes_scanned_to_this_tier",
                        estimated_query_bytes_scanned_to_this_tier,
                        {"tier": str(tier)},
                    )
                    bytes_scanned_limit = self._get_bytes_scanned_limit()
                    if (
                        estimated_query_bytes_scanned_to_this_tier
                        <= bytes_scanned_limit
                    ):
                        target_tier = tier
                        estimated_target_tier_bytes_scanned = (
                            estimated_query_bytes_scanned_to_this_tier
                        )
                    tier_specific_span.set_data(
                        _SAMPLING_IN_STORAGE_PREFIX + "target_tier", target_tier
                    )
                    tier_specific_span.set_data(
                        _SAMPLING_IN_STORAGE_PREFIX + "bytes_scanned_limit",
                        bytes_scanned_limit,
                    )
            self.metrics.increment(
                _SAMPLING_IN_STORAGE_PREFIX + "target_tier",
                1,
                {"tier": str(target_tier)},
            )
            span.set_data(_SAMPLING_IN_STORAGE_PREFIX + "target_tier", target_tier)
            span.set_data(
                _SAMPLING_IN_STORAGE_PREFIX + "estimated_target_tier_bytes_scanned",
                estimated_target_tier_bytes_scanned,
            )
            routing_context.extra_info[
                "estimated_target_tier_bytes_scanned"
            ] = estimated_target_tier_bytes_scanned
            return target_tier

    def _run_query_on_most_downsampled_tier(
        self, routing_context: RoutingContext
    ) -> QueryResult:
        with sentry_sdk.start_span(op="_run_query_on_most_downsampled_tier") as span:
            routing_context.query_settings.set_sampling_tier(
                self._get_most_downsampled_tier()
            )
            request_to_most_downsampled_tier = self._build_snuba_request(
                routing_context
            )
            res = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=request_to_most_downsampled_tier,
                timer=routing_context.timer,
            )

            most_downsampled_query_bytes_scanned = self._get_query_bytes_scanned(res)
            if most_downsampled_query_bytes_scanned == 0:
                self.metrics.increment(
                    "scanned_zero_bytes_in_most_downsampled_tier",
                    1,
                    {"tier": str(self._get_most_downsampled_tier())},
                )

            self._record_value_in_span_and_DD(
                routing_context,
                self.metrics.timing,
                "query_bytes_scanned_from_most_downsampled_tier",
                self._get_query_bytes_scanned(res, span),
            )
            return res

    @with_span(op="function")
    def _decide_tier_and_query_settings(
        self, routing_context: RoutingContext
    ) -> tuple[Tier, ClickhouseQuerySettings]:
        if (
            not routing_context.in_msg.meta.HasField("downsampled_storage_config")
            or routing_context.in_msg.meta.downsampled_storage_config.mode
            == DownsampledStorageConfig.MODE_UNSPECIFIED
        ):
            return Tier.TIER_1, {}

        routing_context.query_result = self._run_query_on_most_downsampled_tier(
            routing_context
        )

        if (
            self._get_query_bytes_scanned(routing_context.query_result)
            >= self._get_bytes_scanned_limit()
        ):
            self._record_value_in_span_and_DD(
                routing_context,
                self.metrics.increment,
                "most_downsampled_tier_query_bytes_scanned_exceeds_limit",
                1,
            )
            return self._get_most_downsampled_tier(), {}

        return (
            self._get_target_tier(routing_context.query_result, routing_context),
            {},
        )

    def _emit_estimation_error_info(
        self,
        routing_context: RoutingContext,
        tags: Dict[str, str],
    ) -> None:
        assert routing_context.query_result
        actual = self._get_query_bytes_scanned(routing_context.query_result)
        if actual == 0:
            self.metrics.increment(
                "scanned_zero_bytes_in_target_tier",
                1,
                tags,
            )
            return

        estimated = routing_context.extra_info.get(
            "estimated_target_tier_bytes_scanned"
        )
        if estimated is None:
            return

        error = estimated - actual
        error_pct = abs(error) / actual

        self._record_value_in_span_and_DD(
            routing_context,
            self.metrics.distribution,
            "estimation_error_percentage",
            error_pct,
            tags,
        )
        self._record_value_in_span_and_DD(
            routing_context,
            self.metrics.distribution,
            "over_estimation_error" if error > 0 else "under_estimation_error",
            abs(error),
            tags,
        )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        assert routing_context.query_result
        target_tier = routing_context.query_settings.get_sampling_tier()

        if self._is_normal_mode(routing_context):
            self._emit_estimation_error_info(
                routing_context, {"tier": str(target_tier)}
            )
