from functools import partial
from typing import Mapping, Optional

from snuba.datasets.entities import EntityKey
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.data_source.join import entity_from_node
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.joins.pre_processor import QualifiedCol, get_equivalent_columns


def add_equivalent_conditions(query: CompositeQuery[Entity]) -> None:
    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        add_equivalent_conditions(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return

    # Now this has to be a join, so we can work with it.

    alias_to_entity = {
        alias: entity_from_node(node)
        for alias, node in from_clause.get_alias_node_map().items()
    }
    entity_to_alias = {}
    for alias, entity in alias_to_entity.items():
        assert (
            entity not in entity_to_alias
        ), f"Unprocessable join condition. Entity {entity} is present more than once"
        entity_to_alias[entity] = alias

    column_equivalence = get_equivalent_columns(from_clause)
    condition = query.get_condition_from_ast()
    if condition is None:
        return
    and_components = get_first_level_and_conditions(condition)
    conditions_to_add = []
    for sub_condition in and_components:
        sole_column = _classify_single_column_condition(sub_condition, alias_to_entity)
        if sole_column is not None:
            for equivalent in column_equivalence.get(sole_column, []):
                replacer = partial(
                    _replace_col, alias_to_entity, entity_to_alias, sole_column
                )
                conditions_to_add.append(sub_condition.transform(replacer))

    query.set_ast_condition(
        combine_and_conditions([*and_components, *conditions_to_add])
    )


def _classify_single_column_condition(
    condition: Expression, alias_entity_map: Mapping[str, EntityKey]
) -> Optional[QualifiedCol]:
    """
    Inspects a condition to check if it is a condition on a
    single column on a single entity
    """
    qualified_col: Optional[QualifiedCol] = None
    for e in condition:
        if isinstance(e, Column):
            if not e.table_name:
                return None
            qualified = QualifiedCol(alias_entity_map[e.table_name], e.column_name)
            if qualified_col is not None and qualified_col != qualified:
                return None
            qualified_col = qualified
    return qualified_col


def _replace_col(
    alias_entity_map: Mapping[str, EntityKey],
    entity_alias_map: Mapping[EntityKey, str],
    old_col: QualifiedCol,
    new_col: QualifiedCol,
    expression: Expression,
) -> Expression:
    if not isinstance(expression, Column):
        return expression

    if (
        expression.column_name != old_col.column
        or expression.table_name is None
        or expression.table_name not in alias_entity_map
        or alias_entity_map[expression.table_name] != old_col.entity
    ):
        return expression

    # Leaving the alias intentionally empty because this expression is
    # not replacing the old one in the query. A brand new condition will
    # be added with the new column, which should not conflict with the
    # old column that will still be in the query.
    return Column(None, entity_alias_map[new_col.entity], new_col.column)
