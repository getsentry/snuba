from typing import Type

from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_create_subscription_pb2 import (
    CreateSubscriptionResponse,
)

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.v1.endpoint_time_series import _convert_aggregations_to_expressions


class CreateSubscriptionRequest(
    RPCEndpoint[CreateSubscriptionRequestProto, CreateSubscriptionResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[CreateSubscriptionRequestProto]:
        return CreateSubscriptionRequestProto

    @classmethod
    def response_class(cls) -> Type[CreateSubscriptionResponse]:
        return CreateSubscriptionResponse

    def _execute(
        self, in_msg: CreateSubscriptionRequestProto
    ) -> CreateSubscriptionResponse:
        from snuba.subscriptions.data import RPCSubscriptionData
        from snuba.subscriptions.subscription import SubscriptionCreator

        # convert aggregations to expressions
        in_msg.time_series_request.CopyFrom(
            _convert_aggregations_to_expressions(in_msg.time_series_request)
        )

        dataset = PluggableDataset(name="eap", all_entities=[])
        entity_key = EntityKey("eap_items_spans")

        subscription = RPCSubscriptionData.from_proto(in_msg, entity_key=entity_key)
        identifier = SubscriptionCreator(dataset, entity_key).create(
            subscription, self._timer
        )

        return CreateSubscriptionResponse(subscription_id=str(identifier))
