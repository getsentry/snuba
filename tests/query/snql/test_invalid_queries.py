import re
from typing import Optional

import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
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
        ParsingException,
        "Parsing error on line 1 at '7 WHEREa<3 OR'",
        id="ORDER BY is two words",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d,2+7 WHERE a<3  ORDER BY fDESC",
        ParsingException,
        "Parsing error on line 1 at '  ORDER BY fD'",
        id="Expression before ASC / DESC needs to be separated from ASC / DESC keyword by space",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d, ,2+7 WHERE a<3  ORDER BY f DESC",
        ParsingException,
        "Parsing error on line 1 at 'c BY d, ,2+7 '",
        id="In a list, columns are separated by exactly one comma",
    ),
    pytest.param(
        "MATCH (events) SELECT 4-5, 3*g(c), c BY d, 2+7 WHERE a<3ORb>2  ORDER BY f DESC",
        ParsingException,
        "Parsing error on line 1 at '<3ORb>2  ORDE'",
        id="mandatory spacing",
    ),
    pytest.param(
        """MATCH (e: events) -[nonsense]-> (t: transactions) SELECT 4-5, e.c
        WHERE e.project_id = 1 AND e.timestamp > toDateTime('2021-01-01') AND t.project_id = 1 AND t.finish_ts > toDateTime('2021-01-01')""",
        ParsingException,
        "ParsingException: events does not have a join relationship -[nonsense]->",
        id="invalid relationship name",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c",
        ParsingException,
        "missing >/>= condition on column timestamp for entity events",
        id="simple query missing required conditions",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = '1' AND e.timestamp >= toDateTime('2021-01-01') AND e.timestamp <= toDateTime('2021-01-02')",
        ParsingException,
        "missing >/>= condition on column finish_ts for entity transactions",
        id="simple query required conditions have wrong type",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1",
        ParsingException,
        "missing >/>= condition on column timestamp for entity events",
        id="simple query missing some required conditions",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c",
        ParsingException,
        "missing >/>= condition on column timestamp for entity events",
        id="join missing required conditions on both sides",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1 AND e.timestamp >= toDateTime('2021-01-01') AND e.timestamp < toDateTime('2021-01-02')",
        ParsingException,
        "missing >/>= condition on column finish_ts for entity transactions",
        id="join missing required conditions on one side",
    ),
    pytest.param(
        "MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c WHERE e.project_id = 1 AND t.finish_ts > toDateTime('2021-01-01') ",
        ParsingException,
        "missing >/>= condition on column timestamp for entity events",
        id="join missing some required conditions on both sides",
    ),
    pytest.param(
        "MATCH { MATCH (events) SELECT count() AS count BY title } SELECT max(count) AS max_count",
        ParsingException,
        "missing >/>= condition on column timestamp for entity events",
        id="subquery missing required conditions",
    ),
]


@pytest.mark.parametrize("query_body, exception, message", test_cases)  # type: ignore
def test_failures(query_body: str, exception: Exception, message: str) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")

    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "assigned": (EntityKey.GROUPASSIGNEE, "group_id"),
        "bookmark": (EntityKey.GROUPEDMESSAGES, "first_release_id"),
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

    events_entity = get_entity(EntityKey.EVENTS)
    setattr(events_entity, "get_join_relationship", events_mock)

    with pytest.raises(exception, match=re.escape(message)):
        parse_snql_query(query_body, events)
