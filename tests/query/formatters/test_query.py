from typing import Union

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import UInt
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities import EntityKey
from snuba.query import (
    LimitBy,
    OrderBy,
    OrderByDirection,
    ProcessableQuery,
    SelectedExpression,
)
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity, Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.formatters.anonymize import format_query as format_query_anonymize
from snuba.query.formatters.tracing import TExpression, format_query
from snuba.query.logical import Query as LogicalQuery
from tests.query.joins.equivalence_schema import EVENTS_SCHEMA, GROUPS_SCHEMA

columns = ColumnSet([("some_int", UInt(8, Modifiers(nullable=True)))])

BASIC_JOIN = JoinClause(
    left_node=IndividualNode(
        alias="ev", data_source=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, None),
    ),
    right_node=IndividualNode(
        alias="gr", data_source=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None),
    ),
    keys=[
        JoinCondition(
            left=JoinConditionExpression("ev", "group_id"),
            right=JoinConditionExpression("gr", "id"),
        )
    ],
    join_type=JoinType.INNER,
)

SIMPLE_SELECT_QUERY = LogicalQuery(
    from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, 0.5),
    selected_columns=[
        SelectedExpression("c1", Column("_snuba_simple", "simple_t", "simple_c")),
    ],
)

LOGICAL_QUERY = LogicalQuery(
    from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, 0.5),
    selected_columns=[
        SelectedExpression("c1", Column("_snuba_c1", "t", "c")),
        SelectedExpression(
            "f1", FunctionCall("_snuba_f1", "f", (Column(None, "t", "c2"),))
        ),
    ],
    array_join=Column(None, None, "col"),
    condition=binary_condition(
        "equals", Column(None, None, "c4"), Literal(None, "asd")
    ),
    groupby=[Column(None, "t", "c4")],
    having=binary_condition("equals", Column(None, None, "c6"), Literal(None, "asd2")),
    order_by=[OrderBy(OrderByDirection.ASC, Column(None, "t", "c"))],
    limitby=LimitBy(100, Column(None, None, "c8")),
    limit=150,
)


SIMPLE_FORMATTED = [
    "SELECT",
    "  t.c AS `_snuba_c1` |> c1,",
    "  f(",
    "    t.c2",
    "  ) AS `_snuba_f1` |> f1",
    "FROM",
    "  Entity(events) SAMPLE 0.5",
    "GROUPBY",
    "  t.c4",
    "ORDER_BY",
    "  t.c ASC",
    "ARRAYJOIN",
    "  col",
    "WHERE",
    "  equals(",
    "    c4,",
    "    'asd'",
    "  )",
    "HAVING",
    "  equals(",
    "    c6,",
    "    'asd2'",
    "  )",
    "LIMIT 100 BY   c8",
    "  LIMIT 150",
]

SIMPLE_ANONYMIZED = (
    "SELECT t.c AS `_snuba_c1`, f( t.c2 ) AS `_snuba_f1`"
    " GROUPBY t.c4 ORDER BY t.c ASC ARRAY JOIN col WHERE equals( c4, $S )"
    " HAVING equals( c6, $S ) LIMIT 100 BY c8 LIMIT 150 OFFSET 0 FROM Entity(events) SAMPLE 0.5"
)

TEST_JOIN = [
    pytest.param(
        LOGICAL_QUERY, SIMPLE_FORMATTED, SIMPLE_ANONYMIZED, id="Simple logical query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=LOGICAL_QUERY,
            selected_columns=[
                SelectedExpression(
                    "f", FunctionCall("f", "avg", (Column(None, "t", "c"),))
                )
            ],
        ),
        [
            "SELECT",
            "  avg(",
            "    t.c",
            "  ) AS `f` |> f",
            "FROM",
            "  SELECT",
            "    t.c AS `_snuba_c1` |> c1,",
            "    f(",
            "      t.c2",
            "    ) AS `_snuba_f1` |> f1",
            "  FROM",
            "    Entity(events) SAMPLE 0.5",
            "  GROUPBY",
            "    t.c4",
            "  ORDER_BY",
            "    t.c ASC",
            "  ARRAYJOIN",
            "    col",
            "  WHERE",
            "    equals(",
            "      c4,",
            "      'asd'",
            "    )",
            "  HAVING",
            "    equals(",
            "      c6,",
            "      'asd2'",
            "    )",
            "  LIMIT 100 BY   c8",
            "    LIMIT 150",
        ],
        (
            "SELECT avg( t.c ) AS `f` OFFSET 0 FROM (SELECT t.c AS `_snuba_c1`, f( t.c2 ) AS `_snuba_f1`"
            " GROUPBY t.c4 ORDER BY t.c ASC ARRAY JOIN col WHERE equals( c4, $S )"
            " HAVING equals( c6, $S ) LIMIT 100 BY c8 LIMIT 150 OFFSET 0 FROM Entity(events) SAMPLE 0.5)"
        ),
        id="Nested Query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression("c1", Column("_snuba_c1", "ev", "c")),
                SelectedExpression(
                    "f1", FunctionCall("_snuba_f1", "f", (Column(None, "ev", "c2"),))
                ),
            ],
        ),
        [
            "SELECT",
            "  ev.c AS `_snuba_c1` |> c1,",
            "  f(",
            "    ev.c2",
            "  ) AS `_snuba_f1` |> f1",
            "FROM",
            "    ['Entity(events)'] AS `ev`",
            "  INNER JOIN",
            "    ['Entity(groupedmessage)'] AS `gr`",
            "  ON",
            "    ev.group_id",
            "    gr.id",
        ],
        (
            "SELECT ev.c AS `_snuba_c1`, f( ev.c2 ) AS `_snuba_f1` OFFSET 0 FROM"
            " LEFT ev, Entity(events) TYPE JoinType.INNER RIGHT gr, Entity(groupedmessage) ON ev.group_id gr.id"
        ),
        id="Basic Join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=CompositeQuery(
                from_clause=CompositeQuery(
                    from_clause=SIMPLE_SELECT_QUERY,
                    selected_columns=[
                        SelectedExpression(
                            "f", FunctionCall("f", "avg", (Column(None, "t", "c"),))
                        )
                    ],
                ),
                selected_columns=[SelectedExpression("tc", Column(None, "t", "c"))],
            ),
            selected_columns=[SelectedExpression("tctop", Column(None, "t", "c"))],
        ),
        [
            "SELECT",
            "  t.c |> tctop",
            "FROM",
            "  SELECT",
            "    t.c |> tc",
            "  FROM",
            "    SELECT",
            "      avg(",
            "        t.c",
            "      ) AS `f` |> f",
            "    FROM",
            "      SELECT",
            "        simple_t.simple_c AS `_snuba_simple` |> c1",
            "      FROM",
            "        Entity(events) SAMPLE 0.5",
        ],
        (
            "SELECT t.c OFFSET 0 FROM (SELECT t.c OFFSET 0 FROM (SELECT avg( t.c ) AS `f`"
            " OFFSET 0 FROM (SELECT simple_t.simple_c AS `_snuba_simple` OFFSET 0 FROM Entity(events) SAMPLE 0.5)))"
        ),
        id="Multiple nestings",
    ),
    pytest.param(
        ClickhouseQuery(
            Table("events", columns),
            selected_columns=[
                SelectedExpression(
                    "tags[promoted_tag]",
                    FunctionCall(
                        "tags[promoted_tag]",
                        "arrayElement",
                        (
                            Column(None, "table", "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, "table", "tags.key"),
                                    Literal(None, "promoted_tag"),
                                ),
                            ),
                        ),
                    ),
                )
            ],
        ),
        [
            "SELECT",
            "  arrayElement(",
            "    table.tags.value,",
            "    indexOf(",
            "      table.tags.key,",
            "      'promoted_tag'",
            "    )",
            "  ) AS `tags[promoted_tag]` |> tags[promoted_tag]",
            "FROM",
            "  Table(events)",
        ],
        "SELECT arrayElement( table.tags.value, indexOf( table.tags.key, $S ) ) AS `tags[promoted_tag]` OFFSET 0 FROM Table(events)",
        id="Clickhouse query",
    ),
]


@pytest.mark.parametrize("query, formatted, formatted_anonymized", TEST_JOIN)
def test_query_formatter(
    query: Union[ProcessableQuery, CompositeQuery[Entity]],
    formatted: TExpression,
    formatted_anonymized: str,
) -> None:
    formatted_query = format_query(query)  # type: ignore
    assert formatted_query == formatted
    formatted_query_anonymized = format_query_anonymize(query)
    assert formatted_query_anonymized.get_sql() == formatted_anonymized
    # make sure there are no empty lines
    assert [line for line in formatted_query if not line] == []
