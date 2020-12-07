from typing import Optional, Mapping
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Expression
from snuba.query.joins.pre_processor import QualifiedCol
from snuba.query.data_source.join import IndividualNode
from snuba.datasets.entities import EntityKey


def add_equivalent_conditions(query: CompositeQuery[Entity]) -> None:
    from_clause = query.get_from_clause()
    if isinstance(from_clause, CompositeQuery):
        add_equivalent_conditions(from_clause)
        return
    elif isinstance(from_clause, ProcessableQuery):
        return

    # Now this has to be a join, so we can work with it.


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
    new_col: QualifiedCol,
    condition: Expression,
) -> Expression:
    # Need to swap the keys
    pass
