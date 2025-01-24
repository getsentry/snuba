import uuid
from collections import defaultdict
from dataclasses import replace
from typing import Any, Callable, Dict, Iterable, Sequence
import sentry_sdk

from clickhouse_driver.errors import Error
from google.protobuf.json_format import MessageToDict
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationComparisonFilter,
    AggregationFilter,
    TraceItemColumnValues,
    TraceItemTableRequest,
    TraceItemTableResponse,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
)

from snuba import environment
from snuba.attribution.appid import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, or_cond
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request as SnubaRequest
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web.query import run_query
from snuba.web.rpc.common.common import (
    apply_virtual_columns,
    attribute_key_to_expression,
    base_conditions_and,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
)
from snuba.web.rpc.common.debug_info import (
    extract_response_meta,
    setup_trace_query_settings,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException, OOMException
from snuba.web.rpc.v1.resolvers import ResolverTraceItemTable
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.aggregation import (
    ExtrapolationContext,
    aggregation_to_expression,
    get_average_sample_rate_column,
    get_confidence_interval_column,
    get_count_column,
)

_DEFAULT_ROW_LIMIT = 10_000

metrics = MetricsWrapper(environment.metrics, "endpoint_trace_item_table")


def aggregation_filter_to_expression(agg_filter: AggregationFilter) -> Expression:
    op_to_expr = {
        AggregationComparisonFilter.OP_LESS_THAN: f.less,
        AggregationComparisonFilter.OP_GREATER_THAN: f.greater,
        AggregationComparisonFilter.OP_LESS_THAN_OR_EQUALS: f.lessOrEquals,
        AggregationComparisonFilter.OP_GREATER_THAN_OR_EQUALS: f.greaterOrEquals,
        AggregationComparisonFilter.OP_EQUALS: f.equals,
        AggregationComparisonFilter.OP_NOT_EQUALS: f.notEquals,
    }

    match agg_filter.WhichOneof("value"):
        case "comparison_filter":
            op_expr = op_to_expr.get(agg_filter.comparison_filter.op)
            if op_expr is None:
                raise BadSnubaRPCRequestException(
                    f"Unsupported aggregation filter op: {AggregationComparisonFilter.Op.Name(agg_filter.comparison_filter.op)}"
                )
            return op_expr(
                aggregation_to_expression(agg_filter.comparison_filter.aggregation),
                agg_filter.comparison_filter.val,
            )
        case "and_filter":
            if len(agg_filter.and_filter.filters) < 2:
                raise BadSnubaRPCRequestException(
                    f"AND filter must have at least two filters, only got {len(agg_filter.and_filter.filters)}"
                )
            return and_cond(
                *(
                    aggregation_filter_to_expression(x)
                    for x in agg_filter.and_filter.filters
                )
            )
        case "or_filter":
            if len(agg_filter.or_filter.filters) < 2:
                raise BadSnubaRPCRequestException(
                    f"OR filter must have at least two filters, only got {len(agg_filter.or_filter.filters)}"
                )
            return or_cond(
                *(
                    aggregation_filter_to_expression(x)
                    for x in agg_filter.or_filter.filters
                )
            )
        case default:
            raise BadSnubaRPCRequestException(
                f"Unsupported aggregation filter type: {default}"
            )


def _convert_order_by(
    order_by: Sequence[TraceItemTableRequest.OrderBy],
) -> Sequence[OrderBy]:
    res: list[OrderBy] = []
    for x in order_by:
        direction = OrderByDirection.DESC if x.descending else OrderByDirection.ASC
        if x.column.HasField("key"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=attribute_key_to_expression(x.column.key),
                )
            )
        elif x.column.HasField("aggregation"):
            res.append(
                OrderBy(
                    direction=direction,
                    expression=aggregation_to_expression(x.column.aggregation),
                )
            )
    return res


def _build_query(request: TraceItemTableRequest) -> Query:
    # TODO: This is hardcoded still
    entity = Entity(
        key=EntityKey("eap_spans"),
        schema=get_entity(EntityKey("eap_spans")).get_data_model(),
        sample=None,
    )

    selected_columns = []
    for column in request.columns:
        if column.HasField("key"):
            key_col = attribute_key_to_expression(column.key)
            # The key_col expression alias may differ from the column label. That is okay
            # the attribute key name is used in the groupby, the column label is just the name of
            # the returned attribute value
            selected_columns.append(
                SelectedExpression(name=column.label, expression=key_col)
            )
        elif column.HasField("aggregation"):
            function_expr = aggregation_to_expression(column.aggregation)
            # aggregation label may not be set and the column label takes priority anyways.
            function_expr = replace(function_expr, alias=column.label)
            selected_columns.append(
                SelectedExpression(name=column.label, expression=function_expr)
            )

            if (
                column.aggregation.extrapolation_mode
                == ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED
            ):
                confidence_interval_column = get_confidence_interval_column(
                    column.aggregation
                )
                if confidence_interval_column is not None:
                    selected_columns.append(
                        SelectedExpression(
                            name=confidence_interval_column.alias,
                            expression=confidence_interval_column,
                        )
                    )

                average_sample_rate_column = get_average_sample_rate_column(
                    column.aggregation
                )
                count_column = get_count_column(column.aggregation)
                selected_columns.append(
                    SelectedExpression(
                        name=average_sample_rate_column.alias,
                        expression=average_sample_rate_column,
                    )
                )
                selected_columns.append(
                    SelectedExpression(name=count_column.alias, expression=count_column)
                )
        else:
            raise BadSnubaRPCRequestException(
                "Column is neither an aggregate or an attribute"
            )
    res = Query(
        from_clause=entity,
        selected_columns=selected_columns,
        condition=base_conditions_and(
            request.meta,
            trace_item_filters_to_expression(request.filter),
        ),
        order_by=_convert_order_by(request.order_by),
        groupby=[
            attribute_key_to_expression(attr_key) for attr_key in request.group_by
        ],
        # protobuf sets limit to 0 by default if it is not set,
        # give it a default value that will actually return data
        limit=request.limit if request.limit > 0 else _DEFAULT_ROW_LIMIT,
        having=aggregation_filter_to_expression(request.aggregation_filter)
        if request.HasField("aggregation_filter")
        else None,
    )
    treeify_or_and_conditions(res)
    apply_virtual_columns(res, request.virtual_column_contexts)
    return res


def _build_snuba_request(request: TraceItemTableRequest) -> SnubaRequest:
    query_settings = (
        setup_trace_query_settings() if request.meta.debug else HTTPQuerySettings()
    )

    return SnubaRequest(
        id=uuid.UUID(request.meta.request_id),
        original_body=MessageToDict(request),
        query=_build_query(request),
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
            parent_api="eap_span_samples",
        ),
    )


def _convert_results(
    request: TraceItemTableRequest, data: Iterable[Dict[str, Any]]
) -> list[TraceItemColumnValues]:

    converters: Dict[str, Callable[[Any], AttributeValue]] = {}

    for column in request.columns:
        if column.HasField("key"):
            if column.key.type == AttributeKey.TYPE_BOOLEAN:
                converters[column.label] = lambda x: AttributeValue(val_bool=bool(x))
            elif column.key.type == AttributeKey.TYPE_STRING:
                converters[column.label] = lambda x: AttributeValue(val_str=str(x))
            elif column.key.type == AttributeKey.TYPE_INT:
                converters[column.label] = lambda x: AttributeValue(val_int=int(x))
            elif column.key.type == AttributeKey.TYPE_FLOAT:
                converters[column.label] = lambda x: AttributeValue(val_float=float(x))
            elif column.key.type == AttributeKey.TYPE_DOUBLE:
                converters[column.label] = lambda x: AttributeValue(val_double=float(x))
        elif column.HasField("aggregation"):
            converters[column.label] = lambda x: AttributeValue(val_double=float(x))
        else:
            raise BadSnubaRPCRequestException(
                "column is neither an attribute or aggregation"
            )

    res: defaultdict[str, TraceItemColumnValues] = defaultdict(TraceItemColumnValues)
    for row in data:
        for column_name, value in row.items():
            if column_name in converters.keys():
                res[column_name].results.append(converters[column_name](value))
                res[column_name].attribute_name = column_name
                extrapolation_context = ExtrapolationContext.from_row(column_name, row)
                if extrapolation_context.is_extrapolated:
                    res[column_name].reliabilities.append(
                        extrapolation_context.reliability
                    )

    column_ordering = {column.label: i for i, column in enumerate(request.columns)}

    return list(
        # we return the columns in the order they were requested
        sorted(
            res.values(), key=lambda c: column_ordering.__getitem__(c.attribute_name)
        )
    )


def _get_page_token(
    request: TraceItemTableRequest, response: list[TraceItemColumnValues]
) -> PageToken:
    if not response:
        return PageToken(offset=0)
    num_rows = len(response[0].results)
    return PageToken(offset=request.page_token.offset + num_rows)


class ResolverTraceItemTableEAPSpans(ResolverTraceItemTable):
    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_SPAN

    def resolve(self, in_msg: TraceItemTableRequest) -> TraceItemTableResponse:
        snuba_request = _build_snuba_request(in_msg)
        try:
            res = run_query(
                dataset=PluggableDataset(name="eap", all_entities=[]),
                request=snuba_request,
                timer=self._timer,
            )
        except Error as e:
            if e.code == 241 or "DB::Exception: Memory limit (for query) exceeded" in e.message:
                metrics.increment("endpoint_trace_item_table_OOM")
                sentry_sdk.capture_exception(e)
            raise BadSnubaRPCRequestException(e.message)

        column_values = _convert_results(in_msg, res.result.get("data", []))
        response_meta = extract_response_meta(
            in_msg.meta.request_id,
            in_msg.meta.debug,
            [res],
            [self._timer],
        )
        return TraceItemTableResponse(
            column_values=column_values,
            page_token=_get_page_token(in_msg, column_values),
            meta=response_meta,
        )
