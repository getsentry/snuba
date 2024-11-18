from datetime import UTC, datetime, timedelta

import pytest
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionResponse,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.error_pb2 import Error
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from tests.base import BaseApiTest
from tests.web.rpc.v1.test_endpoint_time_series import DummyMetric, store_timeseries

END_TIME = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=UTC)
START_TIME = END_TIME - timedelta(hours=1)


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestCreateSubscriptionApi(BaseApiTest):
    def test_create_valid_subscription(self) -> None:
        store_timeseries(
            START_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
        )

        message = CreateSubscriptionRequestProto(
            time_series_request=TimeSeriesRequest(
                meta=RequestMeta(
                    project_ids=[1, 2, 3],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                ),
                aggregations=[
                    AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="test_metric"
                        ),
                        label="sum",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ],
                granularity_secs=300,
            ),
            time_window_secs=300,
            resolution_secs=60,
        )
        response = self.app.post(
            "/rpc/CreateSubscriptionRequest/v1", data=message.SerializeToString()
        )
        assert response.status_code == 200
        response_class = CreateSubscriptionResponse()
        response_class.ParseFromString(response.data)
        assert response_class.subscription_id

    def test_create_invalid_subscription(self) -> None:
        store_timeseries(
            START_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
        )

        message = CreateSubscriptionRequestProto(
            time_series_request=TimeSeriesRequest(
                meta=RequestMeta(
                    project_ids=[1, 2, 3],
                    organization_id=1,
                    cogs_category="something",
                    referrer="something",
                ),
                aggregations=[
                    AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="test_metric"
                        ),
                        label="sum",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ],
                granularity_secs=172800,
            ),
            time_window_secs=172800,
            resolution_secs=60,
        )
        response = self.app.post(
            "/rpc/CreateSubscriptionRequest/v1", data=message.SerializeToString()
        )
        assert response.status_code == 500
        error = Error()
        error.ParseFromString(response.data)
        assert (
            error.message
            == "internal error occurred while executing this RPC call: Time window must be less than or equal to 24 hours"
        )
