from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import and_cond, literal, or_cond
from snuba.query.logical import Query
from snuba.query.snql.parser import _treeify_or_and_conditions

from_dist = QueryEntity(
    EntityKey("generic_metrics_distributions"),
    get_entity(EntityKey("generic_metrics_distributions")).get_data_model(),
)


def test_simple() -> None:
    # from_dist doesnt matter its a dummy value to make query.equals work
    query = Query(
        from_dist,
        condition=and_cond(
            literal(1),
            literal(1),
            literal(1),
            or_cond(
                literal(1),
                or_cond(and_cond(literal(1), literal(1), literal(1)), literal(1)),
                and_cond(literal(1), literal(1), literal(1)),
            ),
        ),
    )
    expected = Query(
        from_dist,
        condition=and_cond(
            and_cond(literal(1), literal(1)),
            and_cond(
                literal(1),
                or_cond(
                    literal(1),
                    or_cond(
                        or_cond(
                            and_cond(literal(1), and_cond(literal(1), literal(1))),
                            literal(1),
                        ),
                        and_cond(literal(1), and_cond(literal(1), literal(1))),
                    ),
                ),
            ),
        ),
    )
    _treeify_or_and_conditions(query)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_composite() -> None:
    """
    Note: does not apply to the conditions of a from_clause subquery (the nested one)
        this is bc transform_expressions is not implemented for composite queries
    """
    query = CompositeQuery(
        from_clause=Query(
            from_dist,
            condition=and_cond(literal(1), literal(1), literal(1)),
        ),
        condition=or_cond(
            and_cond(literal(1), literal(2), literal(3)),
            and_cond(literal(1), literal(2), literal(3)),
            or_cond(literal(1), literal(2)),
        ),
    )
    expected = CompositeQuery(
        from_clause=Query(
            from_dist,
            condition=and_cond(literal(1), literal(1), literal(1)),
        ),
        condition=or_cond(
            and_cond(literal(1), and_cond(literal(2), literal(3))),
            or_cond(
                and_cond(literal(1), and_cond(literal(2), literal(3))),
                or_cond(literal(1), literal(2)),
            ),
        ),
    )
    _treeify_or_and_conditions(query)
    eq, reason = query.equals(expected)
    assert eq, reason
