from __future__ import annotations

from typing import Any, Mapping

from snuba_sdk.conditions import ConditionFunction

from snuba.query.conditions import binary_condition, combine_and_conditions
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery


def resolve_mappings(
    query: LogicalQuery, parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
):
    """
    At the time of writting (Nov 30, 2023), the indexer is called within sentry.
    This might be subjected to change in the future. As a result, this function
    mimics the behavior of the indexer to resolve the metric_id and tag filters
    by using the indexer_mapping provided by the client.
    """
    filters = extract_metric_id(parsed, mql_context)
    filters.extend(extract_resolved_tag_filters(parsed, mql_context))
    query.add_condition_to_ast(combine_and_conditions(filters))
    return query


def extract_metric_id(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    if "mri" not in parsed and "public_name" in parsed:
        public_name = parsed["public_name"]
        mri = mql_context["indexer_mappings"][public_name]
    else:
        mri = parsed["mri"]

    if mri not in mql_context["indexer_mappings"]:
        raise InvalidQueryException(
            "No mri to metric_id mapping found in MQL context indexer_mappings."
        )
    metric_id = mql_context["indexer_mappings"][mri]
    return [
        (
            binary_condition(
                ConditionFunction.EQ.value,
                Column(alias=None, table_name=None, column_name="metric_id"),
                Literal(alias=None, value=metric_id),
            )
        )
    ]


def extract_resolved_tag_filters(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    # Extract resolved tag filters from mql context indexer_mappings
    filters = []
    if "filters" in parsed:
        for filter in parsed["filters"]:
            operator, lhs, rhs = filter
            if lhs in mql_context["indexer_mappings"]:
                resolved = mql_context["indexer_mappings"][lhs]
                lhs_column_name = f"tags_raw[{resolved}]"
            else:
                lhs_column_name = lhs

            if isinstance(rhs, str):
                filters.append(
                    binary_condition(
                        operator,
                        Column(
                            alias=lhs,
                            table_name=None,
                            column_name=lhs_column_name,
                        ),
                        Literal(alias=None, value=rhs),
                    )
                )
            else:
                filters.append(
                    binary_condition(
                        operator,
                        Column(
                            alias=lhs,
                            table_name=None,
                            column_name=lhs_column_name,
                        ),
                        FunctionCall(
                            alias=None,
                            function_name="tuple",
                            parameters=tuple(
                                Literal(alias=None, value=item) for item in rhs
                            ),
                        ),
                    )
                )

    return filters
