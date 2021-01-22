import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities import EntityKey
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.query import SelectedExpression
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
            from_clause=QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
                        (Column(None, None, "column2"), Column(None, None, "column3"),),
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
            from_clause=QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            from_clause=QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
]


@pytest.mark.parametrize("mappers, query, expected", test_cases)
def test_translation(
    mappers: TranslationMappers, query: SnubaQuery, expected: ClickhouseQuery
) -> None:
    translated = QueryTranslator(mappers).translate(query)

    # TODO: consider providing an __eq__ method to the Query class. Or turn it into
    # a dataclass.
    assert (
        expected.get_selected_columns_from_ast()
        == translated.get_selected_columns_from_ast()
    )
    assert expected.get_groupby_from_ast() == translated.get_groupby_from_ast()
    assert expected.get_condition_from_ast() == translated.get_condition_from_ast()
    assert expected.get_arrayjoin_from_ast() == translated.get_arrayjoin_from_ast()
    assert expected.get_having_from_ast() == translated.get_having_from_ast()
    assert expected.get_orderby_from_ast() == translated.get_orderby_from_ast()
