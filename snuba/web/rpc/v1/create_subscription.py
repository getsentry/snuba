from typing import Type

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    CreateSubscriptionsRequest,
    CreateSubscriptionsResponse,
)

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query.data_source.simple import Entity
from snuba.subscriptions.data import RPCSubscriptionData
from snuba.subscriptions.subscription import SubscriptionCreator
from snuba.web.rpc import RPCEndpoint


class EndpointCreateSubscriptions(
    RPCEndpoint[CreateSubscriptionsRequest, CreateSubscriptionsResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[CreateSubscriptionsRequest]:
        return CreateSubscriptionsRequest

    @classmethod
    def response_class(cls) -> Type[CreateSubscriptionsResponse]:
        return CreateSubscriptionsResponse

    def _execute(
        self, in_msg: CreateSubscriptionsRequest
    ) -> CreateSubscriptionsResponse:
        # TODO: This is hardcoded still
        entity = Entity(
            key=EntityKey("eap_spans"),
            schema=get_entity(EntityKey("eap_spans")).get_data_model(),
            sample=None,
        )
        dataset = PluggableDataset(name="eap", all_entities=[])
        entity_key = entity.key
        subscription = RPCSubscriptionData.from_proto(in_msg, entity_key=entity_key)
        identifier = SubscriptionCreator(dataset, entity_key).create(
            subscription, self._timer
        )

        return CreateSubscriptionsResponse(subscription_id=str(identifier))
