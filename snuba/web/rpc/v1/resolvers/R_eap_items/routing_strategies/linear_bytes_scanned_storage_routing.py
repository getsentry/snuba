from dataclasses import asdict
from typing import Any, Callable, Dict, Optional, TypeAlias, Union, cast

import sentry_sdk
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_sdk.tracing import Span

from snuba import state
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.util import with_span
from snuba.web import QueryResult
from snuba.web.query import run_query
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.storage_routing import (
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


class LinearBytesScannedRoutingStrategy(BaseRoutingStrategy):
    def _get_time_budget(self) -> float:
        sentry_timeout_ms = cast(
            int,
            state.get_int_config(
                _SAMPLING_IN_STORAGE_PREFIX + "sentry_timeout", default=30000
            ),
        )  # 30s = 30000ms
        error_budget_ms = cast(
            int,
            state.get_int_config(
                _SAMPLING_IN_STORAGE_PREFIX + "error_budget", default=5000
            ),
        )  # 5s = 5000ms
        return sentry_timeout_ms - error_budget_ms

    def _get_most_downsampled_tier(self) -> Tier:
        return Tier.TIER_64

    def _get_multiplier(self, tier: Tier) -> int:
        return int(
            _DOWNSAMPLING_TIER_MULTIPLIERS[tier]
            / _DOWNSAMPLING_TIER_MULTIPLIERS[self._get_most_downsampled_tier()]
        )

    def _is_preflight_mode(self, routing_context: RoutingContext) -> bool:
        return (
            routing_context.in_msg.meta.HasField("downsampled_storage_config")
            and routing_context.in_msg.meta.downsampled_storage_config.mode
            == DownsampledStorageConfig.MODE_PREFLIGHT
        )

    def _exclude_user_data_from_res(self, res: QueryResult) -> Dict[str, Any]:
        result_dict = asdict(res)
        result_dict["result"] = {
            k: v for k, v in result_dict["result"].items() if k != "data"
        }
        return result_dict

    def _get_query_bytes_scanned(
        self, res: QueryResult, span: Span | None = None
    ) -> int:
        profile_dict = res.result.get("profile") or {}
        progress_bytes = profile_dict.get("progress_bytes")

        if progress_bytes is not None:
            return cast(int, progress_bytes)

        error_type = (
            "QueryResult_contains_no_profile"
            if profile_dict is None
            else "QueryResult_contains_no_progress_bytes"
        )
        res_without_user_data = self._exclude_user_data_from_res(res)
        sentry_sdk.capture_message(
            f"{error_type}: {res_without_user_data}", level="error"
        )
        if span:
            span.set_data(error_type, res_without_user_data)
        return 0

    def _get_query_duration_ms(self, res: QueryResult) -> float:
        return cast(float, res.result.get("profile", {}).get("elapsed", 0) * 1000)  # type: ignore

    def _get_bytes_scanned_limit(self) -> int:
        return cast(
            int,
            state.get_int_config(
                _SAMPLING_IN_STORAGE_PREFIX + "bytes_scanned_per_query_limit",
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
            ] = estimated_query_bytes_scanned_to_this_tier
            return target_tier

    def _run_query_on_most_downsampled_tier(
        self, routing_context: RoutingContext
    ) -> QueryResult:
        with sentry_sdk.start_span(op="_run_query_on_most_downsampled_tier") as span:
            # i dont rly like how this breaks the flow of things can we just get rid of routing_context.target_tier?
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

            self._record_value_in_span_and_DD(
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

        if self._is_preflight_mode(routing_context):
            return self._get_most_downsampled_tier(), {}

        routing_context.query_result = self._run_query_on_most_downsampled_tier(
            routing_context
        )

        query_settings = {
            "max_execution_time": self._get_time_budget() / 1000,
            "timeout_overflow_mode": "break",
        }

        return (
            self._get_target_tier(routing_context.query_result, routing_context),
            query_settings,
        )

    def _run_query(self, routing_context: RoutingContext) -> QueryResult:
        # rachel: in what situation would the target tier be the most downsampled tier and the query result be None?
        if (
            routing_context.query_settings.get_sampling_tier()
            == self._get_most_downsampled_tier()
            and routing_context.query_result is not None
        ):
            return routing_context.query_result
        return super()._run_query(routing_context)

    def _emit_estimation_error_info(
        self,
        routing_context: RoutingContext,
        tags: Dict[str, str],
    ) -> None:
        assert routing_context.query_result
        if self._get_query_bytes_scanned(routing_context.query_result) != 0:
            estimated_target_tier_query_bytes_scanned = routing_context.extra_info[
                "estimated_target_tier_bytes_scanned"
            ]
            estimation_error = (
                estimated_target_tier_query_bytes_scanned
                - self._get_query_bytes_scanned(routing_context.query_result)
            )

            self._record_value_in_span_and_DD(
                self.metrics.distribution,
                "estimation_error_percentage",
                abs(estimation_error)
                / self._get_query_bytes_scanned(routing_context.query_result),
                tags,
            )

            estimation_error_metric_name = (
                "over_estimation_error"
                if estimation_error > 0
                else "under_estimation_error"
            )
            self._record_value_in_span_and_DD(
                self.metrics.distribution,
                estimation_error_metric_name,
                abs(estimation_error),
                tags,
            )

    def _output_metrics(self, routing_context: RoutingContext) -> None:
        # TODO: Test what happens when the query result is an error
        assert routing_context.query_result
        if not self._is_preflight_mode(routing_context):
            target_tier = routing_context.query_settings.get_sampling_tier()
            self._emit_estimation_error_info(
                routing_context, {"tier": str(target_tier)}
            )
            self._record_value_in_span_and_DD(
                self.metrics.distribution,
                f"actual_bytes_scanned_in_target_tier_{target_tier}",
                self._get_query_bytes_scanned(routing_context.query_result),
                tags={"tier": str(target_tier)},
            )
            self._record_value_in_span_and_DD(
                self.metrics.timing,
                f"time_to_run_query_in_target_tier_{target_tier}",
                self._get_query_duration_ms(routing_context.query_result),
                tags={"tier": str(target_tier)},
            )
