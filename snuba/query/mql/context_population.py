from __future__ import annotations

from snuba_sdk.metrics_visitors import AGGREGATE_ALIAS

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.mql.mql_context import MQLContext
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import MAX_LIMIT
from snuba.util import parse_datetime
from snuba.utils.constants import GRANULARITIES_AVAILABLE


def start_end_time_condition(
    mql_context: MQLContext, entity_key: EntityKey, table_name: str | None = None
) -> Expression:
    try:
        start = parse_datetime(mql_context.start)
        end = parse_datetime(mql_context.end)
    except Exception as e:
        raise ParsingException("Invalid start or end time") from e

    entity = get_entity(entity_key)
    required_timestamp_column = (
        entity.required_time_column if entity.required_time_column else "timestamp"
    )
    filters = []
    filters.append(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, table_name, column_name=required_timestamp_column),
            Literal(None, value=start),
        ),
    )
    filters.append(
        binary_condition(
            ConditionFunctions.LT,
            Column(None, table_name, column_name=required_timestamp_column),
            Literal(None, value=end),
        ),
    )
    return combine_and_conditions(filters)


def scope_conditions(mql_context: MQLContext, table_name: str | None = None) -> Expression:
    filters = []
    filters.append(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, table_name, "project_id"),
            FunctionCall(
                None,
                "tuple",
                tuple(Literal(None, project_id) for project_id in mql_context.scope.project_ids),
            ),
        )
    )
    filters.append(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, table_name, "org_id"),
            FunctionCall(
                None,
                "tuple",
                tuple(Literal(None, int(org_id)) for org_id in mql_context.scope.org_ids),
            ),
        )
    )
    filters.append(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, table_name, "use_case_id"),
            Literal(None, mql_context.scope.use_case_id),
        )
    )
    return combine_and_conditions(filters)


def rollup_expressions(
    mql_context: MQLContext, table_name: str | None = None
) -> tuple[Expression, bool, OrderBy | None, SelectedExpression | None]:
    """
    This function returns four values based on the rollup field in the MQL context:
    - granularity_condition: an expression that filters the granularity column based on the granularity in the MQL context
    - with_totals: a boolean indicating whether the query should include totals
    - orderby: an OrderBy object that specifies the ordering of the results
    - selected_time: a SelectedExpression for the time column in the time series. If this is a total query, this will be None
    """
    rollup = mql_context.rollup

    # Validate/populate granularity
    if rollup.granularity not in GRANULARITIES_AVAILABLE:
        raise ParsingException(
            f"granularity '{rollup.granularity}' is not valid, must be one of {GRANULARITIES_AVAILABLE}"
        )

    granularity_condition = binary_condition(
        ConditionFunctions.EQ,
        Column(None, table_name, "granularity"),
        Literal(None, rollup.granularity),
    )

    # Validate totals/orderby
    if rollup.with_totals is not None and rollup.with_totals not in ("True", "False"):
        raise ParsingException("with_totals must be a string, either 'True' or 'False'")
    if rollup.orderby is not None and rollup.orderby not in ("ASC", "DESC"):
        raise ParsingException("orderby must be either 'ASC' or 'DESC'")
    if rollup.interval is not None and rollup.orderby is not None:
        raise ParsingException("orderby is not supported when interval is specified")
    if rollup.interval and (
        rollup.interval < GRANULARITIES_AVAILABLE[0] or rollup.interval < rollup.granularity
    ):
        raise ParsingException(
            f"interval {rollup.interval} must be greater than or equal to granularity {rollup.granularity}"
        )

    with_totals = rollup.with_totals == "True"
    selected_time = None
    orderby = None

    if rollup.interval:
        # If an interval is specified, then we need to group the time by that interval,
        # return the time in the select, and order the results by that time.
        prefix = "" if not table_name else f"{table_name}."
        time_expression = FunctionCall(
            f"{prefix}time",
            "toStartOfInterval",
            parameters=(
                Column(None, table_name, "timestamp"),
                FunctionCall(
                    None,
                    "toIntervalSecond",
                    (Literal(None, rollup.interval),),
                ),
                Literal(None, "Universal"),
            ),
        )
        selected_time = SelectedExpression("time", time_expression)
        orderby = OrderBy(OrderByDirection.ASC, time_expression)
    elif rollup.orderby is not None:
        direction = OrderByDirection.ASC if rollup.orderby == "ASC" else OrderByDirection.DESC
        orderby = OrderBy(direction, Column(None, None, AGGREGATE_ALIAS))

    return granularity_condition, with_totals, orderby, selected_time


def limit_value(mql_context: MQLContext) -> int:
    limit = 1000
    if mql_context.limit:
        if mql_context.limit > MAX_LIMIT:
            raise ParsingException(
                "queries cannot have a limit higher than 10000", should_report=False
            )
        limit = mql_context.limit

    return limit


def offset_value(mql_context: MQLContext) -> int | None:
    if mql_context.offset:
        if mql_context.offset < 0:
            raise ParsingException("offset must be greater than or equal to 0")
        return mql_context.offset

    return None
