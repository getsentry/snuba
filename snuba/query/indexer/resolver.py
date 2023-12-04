from __future__ import annotations

from typing import Any, Mapping, Union

from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.mql.mql_context import MQLContext


def resolve_mappings(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
) -> Union[CompositeQuery[QueryEntity], LogicalQuery]:
    """
    At the time of writting (Nov 30, 2023), the indexer is called within sentry.
    This might be subjected to change in the future. As a result, this function
    mimics the behavior of the indexer to resolve the metric_id and tag filters
    by using the indexer_mapping provided by the client.
    """
    resolve_metric_id_processor(query, parsed, mql_context)
    resolve_tag_filters_processor(query, parsed, mql_context)
    resolve_gropupby_processor(query, parsed, mql_context)
    return query


def resolve_metric_id_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
) -> None:
    """
    Adds the resolved metric_id to the AST conditions
    """
    if "mri" not in parsed and "public_name" in parsed:
        public_name = parsed["public_name"]
        mri = mql_context.indexer_mappings[public_name]
    else:
        mri = parsed["mri"]

    if mri not in mql_context.indexer_mappings:
        raise InvalidQueryException(
            "No mri to metric_id mapping found in MQL context indexer_mappings."
        )
    metric_id = mql_context.indexer_mappings[mri]
    query.add_condition_to_ast(
        binary_condition(
            ConditionFunctions.EQ,
            Column(alias=None, table_name=None, column_name="metric_id"),
            Literal(alias=None, value=metric_id),
        )
    )


def resolve_tag_filters_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
) -> None:
    """
    Traverse through all conditions of the AST,
    then finds and replaces tag filters with resolved names
    """
    conditions = parsed.get("filters", None)
    if conditions:
        for condition in conditions:
            assert isinstance(condition, FunctionCall)
            column = condition.parameters[0]  # lhs
            assert isinstance(column, Column)
            column_name = column.column_name
            if column_name in mql_context.indexer_mappings:
                resolved = mql_context.indexer_mappings[column_name]
                lhs_column_name = f"tags_raw[{resolved}]"
                replace_column = Column(
                    alias=column_name,
                    table_name=None,
                    column_name=lhs_column_name,
                )
                query.find_and_replace_column_in_condition(column_name, replace_column)


def resolve_gropupby_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
) -> None:
    """
    Iterates through the groupby and selected_columns in AST,
    then finds and replaces the groupby column with resolved names
    """
    groupbys = parsed.get("groupby", None)
    if groupbys:
        for groupby_column in groupbys:
            assert isinstance(groupby_column, Column)
            if groupby_column.column_name in mql_context.indexer_mappings:
                resolved = mql_context.indexer_mappings[groupby_column.column_name]
                resolved_column_name = f"tags_raw[{resolved}]"
                column = Column(
                    alias=groupby_column.column_name,
                    table_name=None,
                    column_name=resolved_column_name,
                )
                query.find_and_replace_column_in_groupby_and_selected_columns(
                    groupby_column.column_name, column
                )
