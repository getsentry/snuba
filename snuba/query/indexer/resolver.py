from __future__ import annotations

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


def resolve_tag_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mapping: dict[str, str | int],
) -> None:
    def resolve_tag_column(exp: Expression) -> Expression:
        if isinstance(exp, Column) and exp.column_name in indexer_mapping:
            return Column(
                alias=exp.alias,
                table_name=exp.table_name,
                # TODO: Metrics (non-generic metrics) queries use `tags` instead of `tags_raw`
                column_name=f"tags_raw[{resolve(exp.column_name, indexer_mapping)}]",
            )
        return exp

    query.transform_expressions(resolve_tag_column)


def resolve_metric_id(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    indexer_mappings: dict[str, str | int],
) -> None:
    def resolve_metric_id_column(exp: Expression) -> Expression:
        # Metric IDs are built into the queries as conditions, e.g. metric_id = X
        # However X could be a public name ("transaction.duration") or an MRI ("d:transactions/duration@millisecond")
        # Resolve those strings down to the integer metric ID.

        if isinstance(exp, Literal) and exp.value in indexer_mappings:
            # This could be a public_name or an mri. public_name -> mri -> metric_id
            # So we need to resolve the MRI potentially as well.
            mapping = resolve(str(exp.value), indexer_mappings)
            if isinstance(mapping, int):
                metric_id = mapping
            else:
                metric_id = int(resolve(mapping, indexer_mappings))

            return Literal(None, metric_id)
        return exp

    query.transform_expressions(resolve_metric_id_column)


def resolve_mappings(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
) -> None:
    """
    At the time of writting (Nov 30, 2023), the indexer is called within sentry.
    This might be subjected to change in the future. As a result, this function
    mimics the behavior of the indexer to resolve the metric_id and tag filters
    by using the indexer_mapping provided by the client.
    """
    resolve_metric_id(query, mappings)
    resolve_tag_mappings(query, mappings)
