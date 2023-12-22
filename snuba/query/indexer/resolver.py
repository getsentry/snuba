from __future__ import annotations

from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, Literal
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
            print(get_dataset_name(dataset))
            if get_dataset_name(dataset) == "metrics":
                column_name = f"tags[{resolve(exp.column_name, indexer_mapping)}]"
            else:
                column_name = f"tags_raw[{resolve(exp.column_name, indexer_mapping)}]"
            return Column(
                alias=exp.alias,
                table_name=exp.table_name,
                column_name=column_name,
            )
        return exp

    query.transform_expressions(resolve_tag_column)


def resolve_tag_value_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mappings: dict[str, str | int],
) -> None:
    """
    This function is responsible for resolving all tag values of a query.

    Metric IDs are built into the queries as conditions, e.g. metric_id = X.
    Additionally, the metrics (release-health) dataset indexes both tag keys
    and tag values. Therefore, we can resolve both the metric_id (which is just a tag value)
    and the filter tag values together since they are both RHS literals.
    """

    def resolve_tag_value(exp: Expression) -> Expression:
        if isinstance(exp, Literal) and exp.value in indexer_mappings:
            mapping = resolve(str(exp.value), indexer_mappings)
            if isinstance(mapping, int):
                value = mapping
            else:
                # We support passing in either public_name or mri in MQL.
                # The indexer_mapping follows the order: public_name -> mri -> metric_id
                # For exmaple: {
                #     "transaction.duration": "d:transactions/duration@millisecond",
                #     "d:transactions/duration@millisecond": 100001,
                # }
                # Therefore, we need to resolve the string down to the integer metric_id
                value = int(resolve(mapping, indexer_mappings))

            return Literal(None, value)
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
    resolve_tag_value_mappings(query, mappings)
