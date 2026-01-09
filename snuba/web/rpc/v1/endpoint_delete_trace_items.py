from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from sentry_protos.snuba.v1.endpoint_delete_trace_items_pb2 import (
    DeleteTraceItemsRequest,
    DeleteTraceItemsResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemFilterWithType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter

from snuba.attribution.appid import AppID
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.bulk_delete_query import delete_from_storage
from snuba.lw_deletions.types import AttributeConditions
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


def _extract_attribute_value(comparison_filter: ComparisonFilter) -> Any:
    """Extract the value from a ComparisonFilter's AttributeValue."""
    value_field = comparison_filter.value.WhichOneof("value")
    if value_field == "val_str":
        return comparison_filter.value.val_str
    elif value_field == "val_int":
        return comparison_filter.value.val_int
    elif value_field == "val_double":
        return comparison_filter.value.val_double
    elif value_field == "val_bool":
        return comparison_filter.value.val_bool
    elif value_field == "val_str_array":
        return list(comparison_filter.value.val_str_array.values)
    elif value_field == "val_int_array":
        return list(comparison_filter.value.val_int_array.values)
    elif value_field == "val_double_array":
        return list(comparison_filter.value.val_double_array.values)
    else:
        raise BadSnubaRPCRequestException(f"Unsupported attribute value type: {value_field}")


def _trace_item_filters_to_attribute_conditions(
    item_type: int,
    filters: Sequence[TraceItemFilterWithType],
) -> AttributeConditions:
    """
    Convert TraceItemFilters to AttributeConditions for deletion.

    Only supports ComparisonFilter with OP_EQUALS or OP_IN operations.
    All filters are combined with AND logic.

    Args:
        item_type: The trace item type (e.g., occurrence, span)
        filters: List of TraceItemFilterWithType from the request

    Returns:
        AttributeConditions object containing item_type and attribute mappings

    Raises:
        BadSnubaRPCRequestException: If unsupported filter types or operations are encountered
    """
    attributes: Dict[str, Tuple[AttributeKey, List[Any]]] = {}

    for filter_with_type in filters:
        # Extract the actual filter from TraceItemFilterWithType
        trace_filter = filter_with_type.filter
        # Only support comparison filters for deletion
        if not trace_filter.HasField("comparison_filter"):
            raise BadSnubaRPCRequestException(
                "Only comparison filters are supported for deletion. "
                "AND, OR, and NOT filters are not supported."
            )

        comparison_filter = trace_filter.comparison_filter
        op = comparison_filter.op

        # Only support equality operations for deletion
        if op not in (ComparisonFilter.OP_EQUALS, ComparisonFilter.OP_IN):
            op_name = ComparisonFilter.Op.Name(op)
            raise BadSnubaRPCRequestException(
                f"Only OP_EQUALS and OP_IN operations are supported for deletion. Got: {op_name}"
            )

        attribute_name = comparison_filter.key.name
        value = _extract_attribute_value(comparison_filter)

        # Convert single values to lists for consistency
        if not isinstance(value, list):
            value = [value]

        # If the attribute already exists, extend the list (OR logic within same attribute)
        if attribute_name in attributes:
            _, existing_values = attributes[attribute_name]
            existing_values.extend(value)
        else:
            attributes[attribute_name] = (comparison_filter.key, value)

    return AttributeConditions(item_type=item_type, attributes=attributes)


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

        # Build base conditions that apply to all deletions
        conditions: Dict[str, List[Any]] = {
            "organization_id": [request.meta.organization_id],
            "project_id": list(request.meta.project_ids),
        }

        attribute_conditions: Optional[AttributeConditions] = None

        if has_trace_ids:
            # Delete by trace_ids (no attribute filtering)
            conditions["trace_id"] = [str(tid) for tid in request.trace_ids]
        else:
            # Delete by filters (with attribute filtering)
            # item_type must be specified in the request metadata for attribute-based deletion
            if request.meta.trace_item_type == 0:  # TRACE_ITEM_TYPE_UNSPECIFIED
                raise BadSnubaRPCRequestException(
                    "trace_item_type must be specified in metadata when using filters"
                )

            attribute_conditions = _trace_item_filters_to_attribute_conditions(
                request.meta.trace_item_type,
                request.filters,
            )
            # Add item_type to conditions for the delete query
            conditions["item_type"] = [attribute_conditions.item_type]

        delete_result = delete_from_storage(
            get_writable_storage(StorageKey.EAP_ITEMS),
            conditions,
            attribution_info,
            attribute_conditions,
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
