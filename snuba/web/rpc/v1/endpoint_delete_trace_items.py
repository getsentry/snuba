from typing import Type

from sentry_protos.snuba.v1.endpoint_delete_trace_items_pb2 import (
    DeleteTraceItemsRequest,
    DeleteTraceItemsResponse,
)

from snuba.attribution.appid import AppID
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.bulk_delete_query import delete_from_storage
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


class EndpointDeleteTraceItems(RPCEndpoint[DeleteTraceItemsRequest, DeleteTraceItemsResponse]):
    @classmethod
    def version(cls) -> str:
        return "v1"

    @classmethod
    def request_class(cls) -> Type[DeleteTraceItemsRequest]:
        return DeleteTraceItemsRequest

    @classmethod
    def response_class(cls) -> Type[DeleteTraceItemsResponse]:
        return DeleteTraceItemsResponse

    def _execute(self, request: DeleteTraceItemsRequest) -> DeleteTraceItemsResponse:
        has_trace_ids = len(request.trace_ids) > 0
        has_filters = len(request.filters) > 0

        if not (has_trace_ids or has_filters):
            raise BadSnubaRPCRequestException("Either trace_ids or filters must be provided.")

        if has_trace_ids and has_filters:
            raise BadSnubaRPCRequestException("Provide only one of trace_ids or filters, not both.")

        if has_filters:
            raise NotImplementedError("Currently, only delete by trace_ids is supported")

        attribution_info = {
            "app_id": AppID("eap"),
            "referrer": request.meta.referrer,
            "tenant_ids": {
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            "team": "eap",
            "feature": "eap",
            "parent_api": "eap_delete_trace_items",
        }

        delete_result = delete_from_storage(
            get_writable_storage(StorageKey.EAP_ITEMS),
            {
                "organization_id": [request.meta.organization_id],
                "trace_id": list(request.trace_ids),
                "project_id": list(request.meta.project_ids),
            },
            attribution_info,
        )

        response = DeleteTraceItemsResponse()
        # TODO: fix how we pass this data around, this is too coupled
        # to the response we give in the Snuba API
        response.matching_items_count = next(
            (
                x["rows_to_delete"]
                for x in delete_result.get("eap_items_1_local", {}).get("data", [])
                if "rows_to_delete" in x
            ),
            0,
        )
        response.meta.request_id = request.meta.request_id

        return response
