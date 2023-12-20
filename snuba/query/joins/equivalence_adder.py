from functools import partial
from typing import Mapping, MutableMapping, Optional, Set, Tuple

from snuba.datasets.entities.entity_key import EntityKey
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.data_source.join import entity_from_node
from snuba.query.data_source.multi import MultiQuery
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.joins.pre_processor import QualifiedCol, get_equivalent_columns


def add_equivalent_conditions(query: CompositeQuery[Entity]) -> None:
    """
    Finds conditions in a join query on columns that have a semantic
    equivalent in another entity in the join and add the same condition
    on the equivalent column.

    Example: In a join between events and groupedmessage, if there is
    a condition on events.project_id, it would replicate the same
    condition on groupedmessage.project_id as this is a semantically
    equivalent column.

    The goal is to reduce the amount of data that is loaded by clickhouse
    for each subquery by adding all the conditions we can to all
    subqueries.

    Cases we skip:
    - top level conditions that include columns in multiple tables.
      These cannot be pushed down to subqueries.
    - top level conditions containing multiple columns as some may
      not have a semantic equivalent. TODO: This can be extended by
      supporting conditions that contain multiple column which all
      have an equivalent in the same entity
    """

    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        add_equivalent_conditions(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return
    elif isinstance(from_clause, MultiQuery):
        return

    # Now this has to be a join, so we can work with it.

    alias_to_entity = {
        alias: entity_from_node(node)
        for alias, node in from_clause.get_alias_node_map().items()
    }
    entity_to_alias: MutableMapping[EntityKey, Set[str]] = {}
    for alias, entity in alias_to_entity.items():
        entity_to_alias.setdefault(entity, set()).add(alias)

    column_equivalence = get_equivalent_columns(from_clause)
    condition = query.get_condition()
    if condition is None:
        return

    and_components = get_first_level_and_conditions(condition)
    conditions_to_add = []
    for sub_condition in and_components:
        # We duplicate only the top level conditions that reference one
        # and only one column that has a semantic equivalent.
        # This excludes top level conditions that contains columns from
        # multiple entities, and cannot be pushed down to subqueries.
        #
        # TODO: Address top level conditions that contain multiple
        # columns each of which has an equivalent in the same entity.
        sole_column = _classify_single_column_condition(sub_condition, alias_to_entity)
        if sole_column is not None:
            column_in_condition, table_alias_in_condition = sole_column

            for equivalent_table_alias in entity_to_alias[column_in_condition.entity]:
                if equivalent_table_alias != table_alias_in_condition:
                    # There are multiple occurrences of the entity found.
                    # Apply the same condition everywhere.
                    replacer = partial(
                        _replace_col,
                        table_alias_in_condition,
                        column_in_condition.column,
                        equivalent_table_alias,
                        column_in_condition.column,
                    )
                    conditions_to_add.append(sub_condition.transform(replacer))

            for equivalent in column_equivalence.get(column_in_condition, []):
                # There are equivalent column on different entities
                # in the query. Transform the condition and add it
                # to all entities.
                equivalent_aliases = entity_to_alias.get(equivalent.entity, set())
                for table_alias in equivalent_aliases:
                    replacer = partial(
                        _replace_col,
                        table_alias_in_condition,
                        column_in_condition.column,
                        table_alias,
                        equivalent.column,
                    )
                    conditions_to_add.append(sub_condition.transform(replacer))

    query.set_ast_condition(
        combine_and_conditions([*and_components, *conditions_to_add])
    )


def _classify_single_column_condition(
    condition: Expression, alias_entity_map: Mapping[str, EntityKey]
) -> Optional[Tuple[QualifiedCol, str]]:
    """
    Inspects a condition to check if it is a condition on a
    single column on a single entity
    """
    qualified_col: Optional[Tuple[QualifiedCol, str]] = None
    for e in condition:
        if isinstance(e, Column):
            if not e.table_name:
                return None
            qualified = (
                QualifiedCol(alias_entity_map[e.table_name], e.column_name),
                e.table_name,
            )
            if qualified_col is not None and qualified_col != qualified:
                return None
            qualified_col = qualified
    return qualified_col


def _replace_col(
    old_entity_alias: str,
    old_col_name: str,
    new_entity_alias: str,
    new_col_name: str,
    expression: Expression,
) -> Expression:
    if (
        not isinstance(expression, Column)
        or expression.column_name != old_col_name
        or expression.table_name is None
        or expression.table_name != old_entity_alias
    ):
        return expression

    # Leaving the alias intentionally empty because this expression is
    # not replacing the old one in the query. A brand new condition will
    # be added with the new column, which should not conflict with the
    # old column that will still be in the query.
    return Column(None, new_entity_alias, new_col_name)
