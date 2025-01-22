from __future__ import annotations

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    Function,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.expressions import (
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    SubscriptableReference,
)
from snuba.web.rpc.common.common import attribute_key_to_expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_FLOATING_POINT_PRECISION = 9


def get_field_existence_expression(aggregation: AttributeAggregation) -> Expression:
    def get_subscriptable_field(field: Expression) -> SubscriptableReference | None:
        """
        Check if the field is a subscriptable reference or a function call with a subscriptable reference as the first parameter to handle the case
        where the field is casting a subscriptable reference (e.g. for integers). If so, return the subscriptable reference.
        """
        if isinstance(field, SubscriptableReference):
            return field
        if isinstance(field, FunctionCall) and len(field.parameters) > 0:
            if len(field.parameters) > 0 and isinstance(
                field.parameters[0], SubscriptableReference
            ):
                return field.parameters[0]

        return None

    field = attribute_key_to_expression(aggregation.key)
    subscriptable_field = get_subscriptable_field(field)
    if subscriptable_field is not None:
        return f.mapContains(subscriptable_field.column, subscriptable_field.key)

    return f.isNotNull(field)


def get_count_column(aggregation: AttributeAggregation) -> Expression:
    return f.sum(
        get_field_existence_expression(aggregation),
        alias="count",
    )


def aggregation_to_expression(aggregation: AttributeAggregation) -> Expression:
    field = attribute_key_to_expression(aggregation.key)
    alias = aggregation.label if aggregation.label else None
    alias_dict = {"alias": alias} if alias else {}
    function_map: dict[Function.ValueType, CurriedFunctionCall | FunctionCall] = {
        Function.FUNCTION_SUM: f.round(
            f.sum(field),
            _FLOATING_POINT_PRECISION,
            **alias_dict,
        ),
        Function.FUNCTION_AVERAGE: f.round(
            f.divide(
                f.sum(field),
                f.sum(get_field_existence_expression(aggregation)),
            ),
            _FLOATING_POINT_PRECISION,
            **alias_dict,
        ),
        Function.FUNCTION_COUNT: f.sum(
            get_field_existence_expression(aggregation),
            **alias_dict,
        ),
        Function.FUNCTION_P50: f.round(
            cf.quantile(0.5)(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_P75: f.round(
            cf.quantile(0.75)(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_P90: f.round(
            cf.quantile(0.9)(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_P95: f.round(
            cf.quantile(0.95)(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_P99: f.round(
            cf.quantile(0.99)(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_AVG: f.round(
            f.avg(field), _FLOATING_POINT_PRECISION, **alias_dict
        ),
        Function.FUNCTION_MAX: f.max(field, **alias_dict),
        Function.FUNCTION_MIN: f.min(field, **alias_dict),
        Function.FUNCTION_UNIQ: f.uniq(field, **alias_dict),
    }

    agg_func_expr = function_map.get(aggregation.aggregate)

    if agg_func_expr is None:
        raise BadSnubaRPCRequestException(
            f"Aggregation not specified for {aggregation.key.name}"
        )

    return agg_func_expr
