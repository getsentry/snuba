import re
from typing import Optional

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.data_source.join import JoinRelationship, JoinType
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query

test_cases = [
    # below are cases that are not parsed completely
    # i.e. the entire string is not consumed
    pytest.param(
        "MATCH (events) SELECT 4-5,3*g(c),c BY d,2+7 WHEREa<3 ORDERBY f DESC",
        "Parsing error on line 1 at '7 WHEREa<3 OR'",
        id="ORDER BY is two words",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d,2+7 WHERE a<3  ORDER BY fDESC",
        "Parsing error on line 1 at '  ORDER BY fD'",
        id="Expression before ASC / DESC needs to be separated from ASC / DESC keyword by space",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d, ,2+7 WHERE a<3  ORDER BY f DESC",
        "Parsing error on line 1 at 'c BY d, ,2+7 '",
        id="In a list, columns are separated by exactly one comma",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d, 2+7 WHERE a<3ORb>2  ORDER BY f DESC",
        "Parsing error on line 1 at '<3ORb>2  ORDE'",
        id="mandatory spacing",
    ),
    pytest.param(
        """MATCH (e: events) -[nonsense]-> (t: transactions) SELECT 4-5, e.c
        WHERE e.project_id = 1 AND e.timestamp > toDateTime('2021-01-01') AND t.project_id = 1 AND t.finish_ts > toDateTime('2021-01-01')""",
        "ParsingException: events does not have a join relationship -[nonsense]->",
        id="invalid relationship name",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c",
        "Missing >= condition with a datetime literal on column timestamp for entity discover. Example: timestamp >= toDateTime('2023-05-16 00:00')",
        id="simple query missing required conditions",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = '1' AND e.timestamp >= toDateTime('2021-01-01') AND e.timestamp <= toDateTime('2021-01-02')",
        "Missing < condition with a datetime literal on column timestamp for entity discover. Example: timestamp < toDateTime('2023-05-16 00:00')",
        id="simple query required conditions have wrong type",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1",
        "Missing >= condition with a datetime literal on column timestamp for entity discover. Example: timestamp >= toDateTime('2023-05-16 00:00')",
        id="simple query missing some required conditions",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c",
        "Missing >= condition with a datetime literal on column timestamp for entity discover. Example: timestamp >= toDateTime('2023-05-16 00:00')",
        id="join missing required conditions on both sides",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1 AND e.timestamp >= toDateTime('2021-01-01') AND e.timestamp < toDateTime('2021-01-02')",
        "missing >= condition on column finish_ts for entity transactions",
        id="join missing required conditions on one side",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1 AND t.finish_ts > toDateTime('2021-01-01') ",
        "Missing >= condition with a datetime literal on column timestamp for entity discover. Example: timestamp >= toDateTime('2023-05-16 00:00')",
        id="join missing some required conditions on both sides",
    ),
    pytest.param(
        "MATCH { MATCH (events) SELECT count() AS count BY title } SELECT max(count) AS max_count",
        "Missing >= condition with a datetime literal on column timestamp for entity discover. Example: timestamp >= toDateTime('2023-05-16 00:00')",
        id="subquery missing required conditions",
    ),
    pytest.param(
        "MATCH (events) SELECT 3*g(c) AS gc, c AS d BY d WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')  ORDER BY f AS g DESC",
        "Parsing error on line 1 at '  ORDER BY f '",
        id="aliases are only in the select",
    ),
    pytest.param(
        "MATCH (discover_events) SELECT arrayMap((x) -> identity(`x`), sdk_integrations) AS sdks WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')",
        "Parsing error on line 1 at 'ap((x) -> ide'",
        id="identifiers have backticks",
    ),
    pytest.param(
        "MATCH (discover_events) SELECT arrayMap((`x`) -> `x`, sdk_integrations) AS sdks WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')",
        "Parsing error on line 1 at 'ap((`x`) -> `'",
        id="ensure function after arrow",
    ),
]


@pytest.mark.parametrize("query_body, message", test_cases)
def test_failures(query_body: str, message: str) -> None:
    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "assigned": (EntityKey.GROUPASSIGNEE, "group_id"),
        "bookmark": (EntityKey.GROUPEDMESSAGE, "first_release_id"),
        "activity": (EntityKey.SESSIONS, "org_id"),
    }

    def events_mock(relationship: str) -> Optional[JoinRelationship]:
        if relationship not in mapping:
            return None
        entity_key, rhs_column = mapping[relationship]
        return JoinRelationship(
            rhs_entity=entity_key,
            join_type=JoinType.INNER,
            columns=[("event_id", rhs_column)],
            equivalences=[],
        )

    events = get_dataset("events")
    events_entity = get_entity(EntityKey.EVENTS)
    setattr(events_entity, "get_join_relationship", events_mock)

    with pytest.raises(ParsingException, match=re.escape(message)):
        parse_snql_query(query_body, events)
