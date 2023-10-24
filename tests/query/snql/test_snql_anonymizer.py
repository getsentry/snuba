import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.snql.parser import parse_snql_query


def build_cond(tn: str) -> str:
    time_column = "finish_ts" if tn == "t" else "timestamp"
    tn = tn + "." if tn else ""
    return f"{tn}project_id=1 AND {tn}{time_column}>=toDateTime('2021-01-01') AND {tn}{time_column}<toDateTime('2021-01-02')"


added_condition = build_cond("")

test_cases = [
    pytest.param(
        f"MATCH (events) SELECT 4-5, c,d,e WHERE {added_condition} LIMIT 5 BY c,d,e",
        (
            "MATCH Entity(events) "
            "SELECT (minus(-1337, -1337) AS `4-5`), c, d, e "
            "WHERE equals(project_id, -1337) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S')) "
            "LIMIT 5 BY c,d,e "
            "LIMIT 1000 OFFSET 0"
        ),
        id="limit by multiple columns",
    ),
    pytest.param(
        f"MATCH (events) SELECT count() AS count BY tags[key], measurements[lcp.elementSize] WHERE measurements[lcp.elementSize] > 1 AND {added_condition}",
        (
            "MATCH Entity(events) "
            "SELECT `tags[key]`, `measurements[lcp.elementSize]`, (count() AS count) "
            "GROUP BY `tags[key]`, `measurements[lcp.elementSize]` "
            "WHERE greater(`measurements[lcp.elementSize]`, -1337) "
            "AND equals(project_id, -1337) AND "
            "greaterOrEquals(timestamp, toDateTime('$S')) AND "
            "less(timestamp, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="Basic query with subscriptables",
    ),
    pytest.param(
        f"MATCH (events) SELECT a WHERE (name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0)) AND {added_condition}",
        (
            "MATCH Entity(events) "
            "SELECT a "
            "WHERE (notEquals(name, bob) "
            "OR less(last_seen, afternoon) "
            "AND (equals(location, gps(x, y, z)) "
            "OR greater(times_seen, -1337))) "
            "AND equals(project_id, -1337) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="Query with multiple / complex conditions joined by parenthesized / regular AND / OR",
    ),
    pytest.param(
        """MATCH (events)
        SELECT a, b[c]
        WHERE project_id IN tuple( 2 , 3)
        AND timestamp>=toDateTime('2021-01-01')
        AND timestamp<toDateTime('2021-01-02')""",
        (
            "MATCH Entity(events) "
            "SELECT a, `b[c]` "
            "WHERE in(project_id, (-1337, -1337)) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="Query with IN condition",
    ),
    pytest.param(
        f"""MATCH (events)
        SELECT 4-5,3*foo(c) AS foo,c
        WHERE or(equals(arrayExists(a, '=', 'RuntimeException'), 1), equals(arrayAll(b, 'NOT IN', tuple('Stack', 'Arithmetic')), 1)) = 1 AND {added_condition}""",
        (
            "MATCH Entity(events) "
            "SELECT (minus(-1337, -1337) AS `4-5`), (multiply(-1337, (foo(c) AS foo)) AS `3*foo(c) AS foo`), c "
            "WHERE equals((equals(arrayExists(a, '$S', '$S'), -1337) "
            "OR equals(arrayAll(b, '$S', ('$S', '$S')), -1337)), -1337) "
            "AND equals(project_id, -1337) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="Special array join functions",
    ),
    pytest.param(
        f"""MATCH
            (e: events) -[contains]-> (t: transactions),
            (e: events) -[assigned]-> (ga: groupassignee)
        SELECT 4-5, ga.c
        WHERE {build_cond('e')} AND {build_cond('t')}""",
        (
            "MATCH "
            "LEFT "
            "LEFT e, Entity(events) "
            "TYPE JoinType.INNER RIGHT ga, Entity(groupassignee)\n ON e.event_id ga.group_id "
            "TYPE JoinType.INNER RIGHT t, Entity(transactions)\n ON e.event_id t.event_id "
            "SELECT (minus(-1337, -1337) AS `4-5`), ga.c "
            "WHERE equals(e.project_id, -1337) "
            "AND greaterOrEquals(e.timestamp, toDateTime('$S')) "
            "AND less(e.timestamp, toDateTime('$S')) "
            "AND equals(t.project_id, -1337) "
            "AND greaterOrEquals(t.finish_ts, toDateTime('$S')) "
            "AND less(t.finish_ts, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="Multi join match",
    ),
    pytest.param(
        "MATCH { MATCH (events) SELECT count() AS count BY title WHERE %s } SELECT max(count) AS max_count"
        % added_condition,
        (
            "MATCH "
            "(MATCH Entity(events) "
            "SELECT title, (count() AS count) "
            "GROUP BY title "
            "WHERE equals(project_id, -1337) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S'))) "
            "SELECT (max(count) AS max_count) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="sub query match",
    ),
    pytest.param(
        f"""MATCH (discover_events)
        SELECT count() AS count BY transaction_name AS tn
        WHERE {added_condition}
        """,
        (
            "MATCH Entity(discover_events) "
            "SELECT transaction_name, (count() AS count) "
            "GROUP BY transaction_name "
            "WHERE equals(project_id, -1337) "
            "AND greaterOrEquals(timestamp, toDateTime('$S')) "
            "AND less(timestamp, toDateTime('$S')) "
            "LIMIT 1000 OFFSET 0"
        ),
        id="aliased columns in select and group by",
    ),
]


@pytest.mark.parametrize("query_body, expected_snql_anonymized", test_cases)
def test_format_expressions(query_body: str, expected_snql_anonymized: str) -> None:
    events = get_dataset("events")
    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "assigned": (EntityKey.GROUPASSIGNEE, "group_id"),
        "bookmark": (EntityKey.GROUPEDMESSAGE, "first_release_id"),
    }

    def events_mock(relationship: str) -> JoinRelationship:
        entity_key, rhs_column = mapping[relationship]
        return JoinRelationship(
            rhs_entity=entity_key,
            join_type=JoinType.INNER,
            columns=[("event_id", rhs_column)],
            equivalences=[],
        )

    events_entity = get_entity(EntityKey.EVENTS)
    setattr(events_entity, "get_join_relationship", events_mock)

    _, snql_anonymized = parse_snql_query(query_body, events)

    assert snql_anonymized == expected_snql_anonymized
