from typing import Type

from sentry_protos.snuba.v1.endpoint_trace_item_table_subscription_pb2 import (
    CreateTraceItemTableSubscriptionRequest as CreateSubscriptionRequestProto,
)
from sentry_protos.snuba.v1.endpoint_trace_item_table_subscription_pb2 import (
    CreateTraceItemTableSubscriptionResponse,
)

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.subscriptions.data import RPCSubscriptionData
from snuba.web.rpc import RPCEndpoint


class CreateTraceItemTableSubscriptionRequest(
    RPCEndpoint[
        CreateSubscriptionRequestProto, CreateTraceItemTableSubscriptionResponse
    ]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[CreateSubscriptionRequestProto]:
        return CreateSubscriptionRequestProto

    @classmethod
    def response_class(cls) -> Type[CreateTraceItemTableSubscriptionResponse]:
        return CreateTraceItemTableSubscriptionResponse

    def _execute(
        self, in_msg: CreateSubscriptionRequestProto
    ) -> CreateTraceItemTableSubscriptionResponse:
        from snuba.subscriptions.subscription import SubscriptionCreator

        dataset = PluggableDataset(name="eap", all_entities=[])
        entity_key = EntityKey("eap_spans")

        subscription = RPCSubscriptionData.from_proto(in_msg, entity_key=entity_key)
        identifier = SubscriptionCreator(dataset, entity_key).create(
            subscription, self._timer
        )

        return CreateTraceItemTableSubscriptionResponse(subscription_id=str(identifier))
