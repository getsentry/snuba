from __future__ import annotations

from dataclasses import replace

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import build_match
from snuba.query.data_source.join import (
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery


def resolve(value: str, mapping: dict[str, str | int]) -> str | int:
    """
    Resolve a value to a new value using a mapping.
    Raise an exception if the value is not in the mapping.
    """
    if value in mapping:
        return mapping[value]

    raise InvalidQueryException(f"Could not resolve {value}")


def resolve_tag_column_name(value: str, mapping: dict[str, str | int], dataset: Dataset) -> str:
    if get_dataset_name(dataset) == "metrics":
        return f"tags[{resolve(value, mapping)}]"
    else:
        return f"tags_raw[{resolve(value, mapping)}]"


def resolve_tag_key_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mapping: dict[str, str | int],
    dataset: Dataset,
) -> None:
    def resolve_tag_column(exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in indexer_mapping:
            column_name = resolve_tag_column_name(exp.column_name, indexer_mapping, dataset)
            return replace(exp, column_name=column_name)
        return exp

    def resolve_join_conditions(
        join_clause: JoinClause[QueryEntity],
    ) -> JoinClause[QueryEntity]:
        # If MQL query is a formula query contain group bys, then the groupby columns will be
        # pushed into the join conditions. Therefore, they must be resolved.
        if isinstance(join_clause.left_node, JoinClause):
            join_clause = replace(
                join_clause, left_node=resolve_join_conditions(join_clause.left_node)
            )
        keys = []
        for join_cond in join_clause.keys:
            left = join_cond.left
            right = join_cond.right
            if left.column in indexer_mapping:
                resolved_name = resolve_tag_column_name(left.column, indexer_mapping, dataset)
                left = JoinConditionExpression(left.table_alias, resolved_name)
            if right.column in indexer_mapping:
                resolved_name = resolve_tag_column_name(right.column, indexer_mapping, dataset)
                right = JoinConditionExpression(right.table_alias, resolved_name)
            new_join_cond = JoinCondition(left, right)
            keys.append(new_join_cond)
        return replace(join_clause, keys=keys)

    query.transform_expressions(resolve_tag_column)
    if isinstance(query, CompositeQuery):
        from_clause = query.get_from_clause()
        if isinstance(from_clause, JoinClause):
            join_clause = resolve_join_conditions(from_clause)
            query.set_from_clause(join_clause)


METRIC_ID_MATCH = build_match(col="metric_id", param_type=str)


def resolve_metric_id_mapping(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mapping: dict[str, str | int],
) -> None:
    def full_resolve(value: str, mappings: dict[str, str | int]) -> int:
        # We support passing in either public_name or mri in MQL.
        # The indexer_mapping follows the order: public_name -> mri -> metric_id
        # For exmaple: {
        #     "transaction.duration": "d:transactions/duration@millisecond",
        #     "d:transactions/duration@millisecond": 100001,
        # }
        # Therefore, we need to resolve the string down to the integer metric_id
        mapping = resolve(value, mappings)
        if isinstance(mapping, str):
            return full_resolve(mapping, mappings)
        return mapping

    def resolve_metric_id(exp: Expression) -> Expression:
        if match := METRIC_ID_MATCH.match(exp):
            rhs = match.expression("rhs")
            lhs = match.expression("column")
            new_rhs: Expression | None = None
            if isinstance(rhs, Literal) and rhs.value in indexer_mapping:
                mapping = full_resolve(str(rhs.value), indexer_mapping)
                new_rhs = replace(rhs, value=mapping)
            elif isinstance(rhs, FunctionCall):  # Array with an IN operator
                new_parameters: list[Expression] = []
                for param in rhs.parameters:
                    if isinstance(param, Literal) and param.value in indexer_mapping:
                        mapping = full_resolve(str(param.value), indexer_mapping)
                        new_parameters.append(replace(param, value=mapping))
                    else:
                        new_parameters.append(param)
                new_rhs = replace(rhs, parameters=tuple(new_parameters))

            if new_rhs is not None:
                assert isinstance(exp, FunctionCall)  # mypy
                return replace(
                    exp,
                    parameters=(
                        lhs,
                        new_rhs,
                    ),
                )

        return exp

    query.transform_expressions(resolve_metric_id)


def resolve_tag_value_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mappings: dict[str, str | int],
) -> None:
    """
    This function is responsible for resolving all tag values of a query.

    The metrics (release-health) dataset indexes both tag keys and tag values.
    """

    def resolve_tag_value(exp: Expression) -> Expression:
        if isinstance(exp, Literal) and exp.value in indexer_mappings:
            mapping = resolve(str(exp.value), indexer_mappings)
            return Literal(None, mapping)
        return exp

    query.transform_expressions(resolve_tag_value)


def resolve_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
    dataset: Dataset,
) -> None:
    """
    At the time of writting (Nov 30, 2023), the indexer is called within sentry.
    This might be subjected to change in the future. As a result, this function
    mimics the behavior of the indexer to resolve the metric_id and tag filters
    by using the indexer_mapping provided by the client.
    """
    resolve_tag_key_mappings(query, mappings, dataset)
    resolve_metric_id_mapping(query, mappings)
    if get_dataset_name(dataset) == "metrics":
        resolve_tag_value_mappings(query, mappings)
