import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToFunctionOnColumn,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as SnubaQuery

test_cases = [
    pytest.param(
        TranslationMappers(),
        SnubaQuery(
            from_clause=QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
            selected_columns=[
                SelectedExpression("alias", Column("alias", "table", "column")),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "f1",
                        (Column(None, None, "column2"), Column(None, None, "column3")),
                    ),
                ),
                SelectedExpression(
                    name=None,
                    expression=SubscriptableReference(
                        None, Column(None, None, "tags"), Literal(None, "myTag")
                    ),
                ),
            ],
        ),
        ClickhouseQuery(
            from_clause=Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("alias", Column("alias", "table", "column")),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "f1",
                        (
                            Column(None, None, "column2"),
                            Column(None, None, "column3"),
                        ),
                    ),
                ),
                SelectedExpression(
                    name=None,
                    expression=SubscriptableReference(
                        None, Column(None, None, "tags"), Literal(None, "myTag")
                    ),
                ),
            ],
        ),
        id="default - no change",
    ),
    pytest.param(
        TranslationMappers(
            columns=[ColumnToColumn(None, "column2", None, "not_column2")],
            subscriptables=[SubscriptableMapper(None, "tags", None, "tags")],
        ),
        SnubaQuery(
            from_clause=QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
            selected_columns=[
                SelectedExpression("alias", Column("alias", "table", "column")),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "f1",
                        (
                            Column("column2", None, "column2"),
                            Column("column3", None, "column3"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "tags[myTag]",
                    SubscriptableReference(
                        "tags[myTag]",
                        Column(None, None, "tags"),
                        Literal(None, "myTag"),
                    ),
                ),
            ],
        ),
        ClickhouseQuery(
            from_clause=Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("alias", Column("alias", "table", "column")),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "f1",
                        (
                            Column("column2", None, "not_column2"),
                            Column("column3", None, "column3"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "tags[myTag]",
                    FunctionCall(
                        "tags[myTag]",
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, None, "tags.key"),
                                    Literal(None, "myTag"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="some basic rules",
    ),
    pytest.param(
        TranslationMappers(
            columns=[
                ColumnToFunction(
                    None,
                    "users_crashed",
                    "uniqIfMerge",
                    (Column(alias=None, table_name=None, column_name="users_crashed"),),
                )
            ],
        ),
        SnubaQuery(
            from_clause=QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "f",
                        (
                            Column(
                                alias=None, table_name=None, column_name="users_crashed"
                            ),
                        ),
                    ),
                ),
            ],
        ),
        ClickhouseQuery(
            from_clause=Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "f",
                        (
                            FunctionCall(
                                None,
                                "uniqIfMerge",
                                (
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="users_crashed",
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="non idempotent rule",
    ),
    pytest.param(
        TranslationMappers(
            columns=[
                ColumnToFunctionOnColumn(
                    None,
                    "tags_key",
                    "arrayJoin",
                    "tags.key",
                )
            ],
        ),
        SnubaQuery(
            from_clause=QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "f",
                        (Column(alias=None, table_name=None, column_name="tags_key"),),
                    ),
                ),
            ],
        ),
        ClickhouseQuery(
            from_clause=Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "f",
                        (
                            FunctionCall(
                                None,
                                "arrayJoin",
                                (
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="tags.key",
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="column to function column",
    ),
    pytest.param(
        TranslationMappers(
            columns=[
                ColumnToFunctionOnColumn(
                    None,
                    "tags_key",
                    "arrayJoin",
                    "tags.key",
                ),
                ColumnToFunctionOnColumn(
                    None,
                    "tags_value",
                    "arrayJoin",
                    "tags.value",
                ),
            ],
        ),
        SnubaQuery(
            from_clause=QueryEntity(EntityKey.EVENTS, EntityColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "platforms",
                    FunctionCall(
                        "platforms",
                        "count",
                        (Column(alias=None, table_name=None, column_name="platform"),),
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    FunctionCall(
                        "top_platforms",
                        "testF",
                        (
                            Column(alias=None, table_name=None, column_name="platform"),
                            Column(
                                alias=None, table_name=None, column_name="tags_value"
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    FunctionCall(
                        "f1_alias",
                        "f1",
                        (
                            Column(alias=None, table_name=None, column_name="tags_key"),
                            Column(alias=None, table_name=None, column_name="column2"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f2_alias",
                    FunctionCall(
                        "alias",
                        "f2",
                        tuple(),
                    ),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                Column(alias=None, table_name=None, column_name="tags_key"),
                Literal(None, "tags_key"),
            ),
            having=binary_condition(
                ConditionFunctions.IN,
                Column(alias=None, table_name=None, column_name="tags_value"),
                Literal(None, "tag"),
            ),
        ),
        ClickhouseQuery(
            from_clause=Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "platforms",
                    FunctionCall(
                        "platforms",
                        "count",
                        (Column(alias=None, table_name=None, column_name="platform"),),
                    ),
                ),
                SelectedExpression(
                    "top_platforms",
                    FunctionCall(
                        "top_platforms",
                        "testF",
                        (
                            Column(alias=None, table_name=None, column_name="platform"),
                            FunctionCall(
                                None,
                                "arrayJoin",
                                (
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="tags.value",
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f1_alias",
                    FunctionCall(
                        "f1_alias",
                        "f1",
                        (
                            FunctionCall(
                                None,
                                "arrayJoin",
                                (
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="tags.key",
                                    ),
                                ),
                            ),
                            Column(alias=None, table_name=None, column_name="column2"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "f2_alias",
                    FunctionCall(
                        "alias",
                        "f2",
                        tuple(),
                    ),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    None,
                    "arrayJoin",
                    (
                        Column(
                            alias=None,
                            table_name=None,
                            column_name="tags.key",
                        ),
                    ),
                ),
                Literal(None, "tags_key"),
            ),
            having=binary_condition(
                ConditionFunctions.IN,
                FunctionCall(
                    None,
                    "arrayJoin",
                    (
                        Column(
                            alias=None,
                            table_name=None,
                            column_name="tags.value",
                        ),
                    ),
                ),
                Literal(None, "tag"),
            ),
        ),
        id="column to function column",
    ),
]


@pytest.mark.parametrize("mappers, query, expected", test_cases)
def test_translation(
    mappers: TranslationMappers, query: SnubaQuery, expected: ClickhouseQuery
) -> None:
    translated = QueryTranslator(mappers).translate(query)
    # basic translate doesn't do this and it's required for the equality
    translated.set_from_clause(Table("my_table", ColumnSet([])))

    eq, reason = expected.equals(translated)
    assert eq, reason
