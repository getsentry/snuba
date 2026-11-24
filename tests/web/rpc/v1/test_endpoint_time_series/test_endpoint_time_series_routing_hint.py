from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import timedelta
from unittest.mock import patch

import pytest
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
    TimeSeriesResponse,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    Function,
)

from snuba.clusters.load_info import LoadInfo
from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.query.allocation_policies import PassthroughPolicy
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.storage_routing.common import decode_routing_hint, encode_routing_hint
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import BaseRoutingStrategy
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from tests.base import BaseApiTest
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    create_mock_routing_decision,
    create_request_meta,
    track_storage_selections,
)


def _count_request(routing_hint: str) -> TimeSeriesRequest:
    meta = create_request_meta(
        BASE_TIME - timedelta(hours=1), BASE_TIME, TraceItemType.TRACE_ITEM_TYPE_SPAN
    )
    # the mode subscriptions send; it is the one that downsamples under load
    meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    return TimeSeriesRequest(
        meta=meta,
        expressions=[
            Expression(
                aggregation=AttributeAggregation(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id"),
                    aggregate=Function.FUNCTION_COUNT,
                    label="count",
                ),
                label="count",
            )
        ],
        granularity_secs=3600,
        routing_hint=routing_hint,
    )


def _execute(
    routed_tier: Tier, routing_hint: str
) -> tuple[EndpointTimeSeries, TimeSeriesResponse, list[StorageKey]]:
    """Runs a count() timeseries query with the routing strategy's decision fixed to routed_tier."""
    message = _count_request(routing_hint)
    storage_keys, storage_tracker = track_storage_selections()
    with storage_tracker, patch.object(RPCEndpoint, "_RPCEndpoint__before_execute"):
        endpoint = EndpointTimeSeries()
        endpoint.routing_decision = create_mock_routing_decision(routed_tier, message)
        response = endpoint.execute(message)
    return endpoint, response, storage_keys


@pytest.mark.eap
@pytest.mark.redis_db
class TestTimeSeriesRoutingHint(BaseApiTest):
    def test_response_hint_names_routed_tier(self) -> None:
        _, response, storage_keys = _execute(Tier.TIER_8, routing_hint="")

        assert decode_routing_hint(response.routing_hint, Tier.TIER_1) == Tier.TIER_8
        assert storage_keys == [StorageKey.EAP_ITEMS_DOWNSAMPLE_8]

    def test_request_hint_overrides_routed_tier(self) -> None:
        endpoint, response, storage_keys = _execute(
            Tier.TIER_1, routing_hint=encode_routing_hint(Tier.TIER_8)
        )

        assert endpoint.routing_decision.tier == Tier.TIER_8
        assert decode_routing_hint(response.routing_hint, Tier.TIER_1) == Tier.TIER_8
        assert storage_keys == [StorageKey.EAP_ITEMS_DOWNSAMPLE_8]

    def test_empty_hint_keeps_routed_tier(self) -> None:
        # Hintless queries have to keep whatever tier the routing strategy picked.
        endpoint, _, storage_keys = _execute(Tier.TIER_8, routing_hint="")

        assert endpoint.routing_decision.tier == Tier.TIER_8
        assert storage_keys == [StorageKey.EAP_ITEMS_DOWNSAMPLE_8]

    def test_invalid_hint_is_bad_request(self) -> None:
        with pytest.raises(BadSnubaRPCRequestException, match="^invalid routing_hint$"):
            _execute(Tier.TIER_1, routing_hint="not-base64!!")


@contextmanager
def _cluster_under_load() -> Iterator[None]:
    """Makes the real routing strategy downsample NORMAL queries from tier 1 to tier 8."""
    with (
        override_options("snuba", {"storage_routing.enable_allocation_policy_downsample": True}),
        patch.object(
            BaseRoutingStrategy,
            "get_allocation_policies",
            return_value=[
                PassthroughPolicy(
                    ResourceIdentifier("EAP"), thresholds={"downsample": {"cluster_load": 1.0}}
                )
            ],
        ),
        patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.get_cluster_loadinfo",
            return_value=LoadInfo(cluster_load=2.0),
        ),
    ):
        yield


@pytest.mark.eap
@pytest.mark.redis_db
class TestTimeSeriesRoutingHintRoute(BaseApiTest):
    """Goes through /rpc/EndpointTimeSeries/v1 with the real routing strategy, nothing mocked
    past the cluster load."""

    def _post(
        self, routing_hint: str, under_load: bool
    ) -> tuple[TimeSeriesResponse, list[StorageKey]]:
        storage_keys, storage_tracker = track_storage_selections()
        with storage_tracker, _cluster_under_load() if under_load else nullcontext():
            http_response = self.app.post(
                "/rpc/EndpointTimeSeries/v1",
                data=_count_request(routing_hint).SerializeToString(),
            )
        assert http_response.status_code == 200, http_response.data
        response = TimeSeriesResponse()
        response.ParseFromString(http_response.data)
        return response, storage_keys

    def test_publishes_tier_1_when_cluster_is_idle(self) -> None:
        response, storage_keys = self._post(routing_hint="", under_load=False)

        assert decode_routing_hint(response.routing_hint, Tier.TIER_8) == Tier.TIER_1
        assert storage_keys == [StorageKey.EAP_ITEMS]

    def test_publishes_tier_8_when_load_downsamples(self) -> None:
        response, storage_keys = self._post(routing_hint="", under_load=True)

        assert decode_routing_hint(response.routing_hint, Tier.TIER_1) == Tier.TIER_8
        assert storage_keys == [StorageKey.EAP_ITEMS_DOWNSAMPLE_8]

    def test_comparison_under_load_follows_live_query_to_tier_1(self) -> None:
        # EAP-988: the live query ran on an idle cluster, the comparison query on a loaded one.
        live, _ = self._post(routing_hint="", under_load=False)

        comparison, storage_keys = self._post(routing_hint=live.routing_hint, under_load=True)

        assert storage_keys == [StorageKey.EAP_ITEMS]
        assert decode_routing_hint(comparison.routing_hint, Tier.TIER_8) == Tier.TIER_1

    def test_comparison_on_idle_cluster_follows_live_query_to_tier_8(self) -> None:
        live, _ = self._post(routing_hint="", under_load=True)

        comparison, storage_keys = self._post(routing_hint=live.routing_hint, under_load=False)

        assert storage_keys == [StorageKey.EAP_ITEMS_DOWNSAMPLE_8]
        assert decode_routing_hint(comparison.routing_hint, Tier.TIER_1) == Tier.TIER_8

    def test_malformed_hint_returns_400(self) -> None:
        http_response = self.app.post(
            "/rpc/EndpointTimeSeries/v1",
            data=_count_request("garbage").SerializeToString(),
        )

        assert http_response.status_code == 400
        error = ErrorProto()
        error.ParseFromString(http_response.data)
        assert error.message == "invalid routing_hint"
