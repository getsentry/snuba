from __future__ import annotations

from dataclasses import replace

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import build_match
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


def resolve_tag_key_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mapping: dict[str, str | int],
    dataset: Dataset,
) -> None:
    def resolve_tag_column(exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in indexer_mapping:
            if get_dataset_name(dataset) == "metrics":
                column_name = f"tags[{resolve(exp.column_name, indexer_mapping)}]"
            else:
                column_name = f"tags_raw[{resolve(exp.column_name, indexer_mapping)}]"
            return replace(exp, column_name=column_name)
        return exp

    query.transform_expressions(resolve_tag_column)


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
