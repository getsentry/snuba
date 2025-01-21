import uuid
from typing import Type

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    AttributeValuesRequest as AttributeValuesRequestProto,
)
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import AttributeValuesResponse

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal, literals_array
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.web.query import run_query
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1alpha.common import (
    base_conditions_and,
    treeify_or_and_conditions,
    truncate_request_meta_to_day,
)


def _build_query(request: AttributeValuesRequestProto) -> Query:
    if request.limit > 1000:
        raise BadSnubaRPCRequestException("Limit can be at most 1000")

    entity = Entity(
        key=EntityKey("spans_str_attrs"),
        schema=get_entity(EntityKey("spans_str_attrs")).get_data_model(),
        sample=None,
    )

    truncate_request_meta_to_day(request.meta)

    res = Query(
        from_clause=entity,
        selected_columns=[
            SelectedExpression(
                name="attr_value",
                expression=f.distinct(column("attr_value", alias="attr_value")),
            ),
        ],
        condition=base_conditions_and(
            request.meta,
            f.equals(column("attr_key"), literal(request.name)),
            # multiSearchAny has special treatment with ngram bloom filters
            # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#functions-support
            f.multiSearchAny(
                column("attr_value"),
                literals_array(None, [literal(request.value_substring_match)]),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC, expression=column("organization_id")
            ),
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_key")),
            OrderBy(direction=OrderByDirection.ASC, expression=column("attr_value")),
        ],
        limit=request.limit,
        offset=request.offset,
    )
    treeify_or_and_conditions(res)
    return res


def _build_snuba_request(
    request: AttributeValuesRequestProto,
) -> SnubaRequest:
    return SnubaRequest(
        id=uuid.uuid4(),
        original_body=MessageToDict(request),
        query=_build_query(request),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="trace_item_values",
        ),
    )


class AttributeValuesRequest(
    RPCEndpoint[AttributeValuesRequestProto, AttributeValuesResponse]
):
    @classmethod
    def version(cls) -> str:
        return "v1alpha"

    @classmethod
    def request_class(cls) -> Type[AttributeValuesRequestProto]:
        return AttributeValuesRequestProto

    def _execute(self, in_msg: AttributeValuesRequestProto) -> AttributeValuesResponse:
        snuba_request = _build_snuba_request(in_msg)
        res = run_query(
            dataset=PluggableDataset(name="eap", all_entities=[]),
            request=snuba_request,
            timer=self._timer,
        )
        return AttributeValuesResponse(
            values=[r["attr_value"] for r in res.result.get("data", [])]
        )
