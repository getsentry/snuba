import uuid
from collections import OrderedDict
from typing import Any, Dict, Iterable, Tuple

from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistribution,
    AttributeDistributions,
    AttributeDistributionsRequest,
    TraceItemStatsRequest,
    TraceItemStatsResponse,
    TraceItemStatsResult,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import (
    arrayJoin,
    column,
    count,
    in_cond,
    literal,
    literals_array,
    not_cond,
    tupleElement,
)
from snuba.query.expressions import FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.constants import ATTRIBUTE_BUCKETS_EAP_ITEMS
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    ATTRIBUTES_TO_EXCLUDE_IN_EAP_ITEMS,
    attribute_key_to_expression_eap_items,
)

_DEFAULT_ROW_LIMIT = 10_000

MAX_BUCKETS = 100
DEFAULT_BUCKETS = 10

COUNT_LABEL = "count()"


def _transform_results(
    results: Iterable[Dict[str, Any]],
    request_meta: RequestMeta,
) -> Iterable[AttributeDistribution]:

    # Maintain the order of keys, so it is in descending order
    # of most prevelant key-value pair.
    res: OrderedDict[Tuple[str, str], AttributeDistribution] = OrderedDict()

    for row in results:
        attr_key = row["attr_key"]
        attr_value = row["attr_value"]
        default = AttributeDistribution(
            attribute_name=attr_key,
        )
        res.setdefault((attr_key, COUNT_LABEL), default).buckets.append(
            AttributeDistribution.Bucket(label=attr_value, value=row[COUNT_LABEL])
        )

    return list(res.values())


def _build_attr_distribution_snuba_request(
    request: TraceItemStatsRequest, query: Query
) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=query,
        query_settings=query_settings,
        attribution_info=AttributionInfo(
            referrer=request.meta.referrer,
            team="eap",
            feature="eap",
            tenant_ids={
                "organization_id": request.meta.organization_id,
                "referrer": request.meta.referrer,
            },
            app_id=AppID("eap"),
            parent_api="eap_attribute_stats",
        ),
    )


def _build_attr_distribution_query(
    in_msg: TraceItemStatsRequest, distributions_params: AttributeDistributionsRequest
) -> Query:
    entity = Entity(
        key=EntityKey("eap_items"),
        schema=get_entity(EntityKey("eap_items")).get_data_model(),
        sample=None,
    )

    concat_attr_maps = FunctionCall(
        alias="attr_str_concat",
        function_name="mapConcat",
        parameters=tuple(
            column(f"attributes_string_{i}") for i in range(ATTRIBUTE_BUCKETS_EAP_ITEMS)
        ),
    )
    attrs_string_keys = tupleElement(
        "attr_key",
        arrayJoin(
            "attributes_string",
            concat_attr_maps,
        ),
        Literal(None, 1),
    )
    attrs_string_values = tupleElement(
        "attr_value",
        arrayJoin(
            "attributes_string",
            concat_attr_maps,
        ),
        Literal(None, 2),
    )

    selected_columns = [
        SelectedExpression(
            name="attr_key",
            expression=attrs_string_keys,
        ),
        SelectedExpression(
            name="attr_value",
            expression=attrs_string_values,
        ),
        SelectedExpression(
            name=COUNT_LABEL,
            expression=count(alias="_count"),
        ),
    ]

    trace_item_filters_expression = trace_item_filters_to_expression(
        in_msg.filter,
        (attribute_key_to_expression_eap_items),
    )

    query = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            in_msg.meta,
            trace_item_filters_expression,
            not_cond(
                in_cond(
                    attrs_string_keys,
                    literals_array(
                        None, list(map(literal, ATTRIBUTES_TO_EXCLUDE_IN_EAP_ITEMS))
                    ),
                ),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.DESC,
                expression=count(),
            ),
        ],
        groupby=[
            attrs_string_keys,
            attrs_string_values,
        ],
        limitby=LimitBy(
            limit=(
                distributions_params.max_buckets
                if distributions_params.max_buckets > 0
                else DEFAULT_BUCKETS
            ),
            columns=[attrs_string_keys],
        ),
        limit=(
            distributions_params.max_attributes
            if distributions_params.max_attributes > 0
            else _DEFAULT_ROW_LIMIT
        ),
    )

    return query


class ResolverTraceItemStatsEAPItems:
    def resolve(
        self,
        in_msg: TraceItemStatsRequest,
        timer: Timer,
    ) -> TraceItemStatsResponse:
        results = []
        for requested_type in in_msg.stats_types:
            result = TraceItemStatsResult()
            if requested_type.HasField("attribute_distributions"):
                if requested_type.attribute_distributions.max_buckets > MAX_BUCKETS:
                    raise BadSnubaRPCRequestException(
                        f"Max allowed buckets is {MAX_BUCKETS}."
                    )

                query = _build_attr_distribution_query(
                    in_msg, requested_type.attribute_distributions
                )
                treeify_or_and_conditions(query)
                snuba_request = _build_attr_distribution_snuba_request(in_msg, query)

                query_res = run_query(
                    dataset=PluggableDataset(name="eap", all_entities=[]),
                    request=snuba_request,
                    timer=timer,
                )

                attributes = _transform_results(
                    query_res.result.get("data", []), in_msg.meta
                )
                result.attribute_distributions.CopyFrom(
                    AttributeDistributions(attributes=attributes)
                )

            results.append(result)

        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [],
            [timer],
        )

        return TraceItemStatsResponse(
            results=results,
            meta=response_meta,
        )
