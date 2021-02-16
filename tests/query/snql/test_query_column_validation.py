import datetime
from typing import Any, Generator

import pytest

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinRelationship,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.snql.parser import parse_snql_query


@pytest.fixture
def set_configs() -> Generator[None, None, None]:
    state.set_config("max_days", 5)
    state.set_config("date_align_seconds", 3600)
    yield
    state.set_config("max_days", None)
    state.set_config("date_align_seconds", 1)


time_validation_tests = [
    pytest.param(
        """MATCH {
            MATCH (events)
            SELECT count() AS count BY title
            WHERE project_id=1
            AND timestamp>=toDateTime('2021-01-01T00:30:00')
            AND timestamp<toDateTime('2021-01-20T00:30:00')
        }
        SELECT max(count) AS max_count""",
        CompositeQuery(
            from_clause=LogicalQuery(
                QueryEntity(
                    EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(),
                ),
                selected_columns=[
                    SelectedExpression("title", Column("_snuba_title", None, "title")),
                    SelectedExpression(
                        "count", FunctionCall("_snuba_count", "count", tuple())
                    ),
                ],
                groupby=[Column("_snuba_title", None, "title")],
                condition=binary_condition(
                    "and",
                    binary_condition(
                        "equals",
                        Column("_snuba_project_id", None, "project_id"),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "greaterOrEquals",
                            Column("_snuba_timestamp", None, "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 15, 0, 0)),
                        ),
                        binary_condition(
                            "less",
                            Column("_snuba_timestamp", None, "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 20, 0, 0)),
                        ),
                    ),
                ),
                limit=1000,
                offset=0,
            ),
            selected_columns=[
                SelectedExpression(
                    "max_count",
                    FunctionCall(
                        "_snuba_max_count",
                        "max",
                        (Column("_snuba_count", None, "_snuba_count"),),
                    ),
                ),
            ],
            limit=1000,
            offset=0,
        ),
        id="subquery has their dates adjusted",
    ),
    pytest.param(
        """MATCH (e: events) -[contains]-> (t: transactions) SELECT 4-5, e.c
        WHERE e.project_id=1
        AND e.timestamp>=toDateTime('2021-01-01T00:30:00')
        AND e.timestamp<toDateTime('2021-01-03T00:30:00')
        AND t.project_id=1
        AND t.finish_ts>=toDateTime('2021-01-01T00:30:00')
        AND t.finish_ts<toDateTime('2021-01-07T00:30:00')""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(),
                    ),
                ),
                right_node=IndividualNode(
                    "t",
                    QueryEntity(
                        EntityKey.TRANSACTIONS,
                        get_entity(EntityKey.TRANSACTIONS).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("t", "event_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("e.c", Column("_snuba_e.c", "e", "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 3, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_t.project_id", "t", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 2, 0, 0)),
                                ),
                                binary_condition(
                                    "less",
                                    Column("_snuba_t.finish_ts", "t", "finish_ts"),
                                    Literal(None, datetime.datetime(2021, 1, 7, 0, 0)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="times are adjusted in each entity",
    ),
    pytest.param(
        """MATCH (e: events) -[connected]-> (s: spans) SELECT 4-5, e.c
        WHERE e.project_id=1
        AND e.timestamp>=toDateTime('2021-01-01T00:30:00')
        AND e.timestamp<toDateTime('2021-01-03T00:30:00')
        AND s.project_id=1
        AND s.timestamp>=toDateTime('2021-01-01T00:30:00')
        AND s.timestamp<toDateTime('2021-01-07T00:30:00')""",
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    "e",
                    QueryEntity(
                        EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model(),
                    ),
                ),
                right_node=IndividualNode(
                    "s",
                    QueryEntity(
                        EntityKey.SPANS, get_entity(EntityKey.SPANS).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression("e", "event_id"),
                        JoinConditionExpression("s", "trace_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5))),
                ),
                SelectedExpression("e.c", Column("_snuba_e.c", "e", "c")),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column("_snuba_e.project_id", "e", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    "and",
                    binary_condition(
                        "greaterOrEquals",
                        Column("_snuba_e.timestamp", "e", "timestamp"),
                        Literal(None, datetime.datetime(2021, 1, 1, 0, 0)),
                    ),
                    binary_condition(
                        "and",
                        binary_condition(
                            "less",
                            Column("_snuba_e.timestamp", "e", "timestamp"),
                            Literal(None, datetime.datetime(2021, 1, 3, 0, 0)),
                        ),
                        binary_condition(
                            "and",
                            binary_condition(
                                "equals",
                                Column("_snuba_s.project_id", "s", "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                "and",
                                binary_condition(
                                    "greaterOrEquals",
                                    Column("_snuba_s.timestamp", "s", "timestamp"),
                                    Literal(None, datetime.datetime(2021, 1, 1, 0, 30)),
                                ),
                                binary_condition(
                                    "less",
                                    Column("_snuba_s.timestamp", "s", "timestamp"),
                                    Literal(None, datetime.datetime(2021, 1, 7, 0, 30)),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="times are adjusted in each entity",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", time_validation_tests)
def test_entity_column_validation(
    query_body: str, expected_query: LogicalQuery, set_configs: Any
) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")

    # TODO: Potentially remove this once entities have actual join relationships
    mapping = {
        "contains": (EntityKey.TRANSACTIONS, "event_id"),
        "connected": (EntityKey.SPANS, "trace_id"),
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

    query = parse_snql_query(query_body, events)

    eq, reason = query.equals(expected_query)
    assert eq, reason
