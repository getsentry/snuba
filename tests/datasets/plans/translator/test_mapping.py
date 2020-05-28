import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import SimpleColumnMapper, TagMapper
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.plans.translator.query import QueryTranslator
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as SnubaQuery

test_cases = [
    (
        "default - no change",
        SnubaQuery(
            body={},
            data_source=TableSource("my_table", ColumnSet([])),
            selected_columns=[
                Column("alias", "table", "column"),
                FunctionCall(
                    "alias2",
                    "f1",
                    (Column(None, None, "column2"), Column(None, None, "column3")),
                ),
                SubscriptableReference(
                    None, Column(None, None, "tags"), Literal(None, "myTag")
                ),
            ],
        ),
        ClickhouseQuery(
            SnubaQuery(
                body={},
                data_source=TableSource("my_table", ColumnSet([])),
                selected_columns=[
                    Column("alias", "table", "column"),
                    FunctionCall(
                        "alias2",
                        "f1",
                        (Column(None, None, "column2"), Column(None, None, "column3")),
                    ),
                    SubscriptableReference(
                        None, Column(None, None, "tags"), Literal(None, "myTag")
                    ),
                ],
            )
        ),
        TranslationMappers(),
    ),
    (
        "some basic rules",
        SnubaQuery(
            body={},
            data_source=TableSource("my_table", ColumnSet([])),
            selected_columns=[
                Column("alias", "table", "column"),
                FunctionCall(
                    "alias2",
                    "f1",
                    (Column(None, None, "column2"), Column(None, None, "column3")),
                ),
                SubscriptableReference(
                    "tags[myTag]", Column(None, None, "tags"), Literal(None, "myTag")
                ),
            ],
        ),
        ClickhouseQuery(
            SnubaQuery(
                body={},
                data_source=TableSource("my_table", ColumnSet([])),
                selected_columns=[
                    Column("alias", "table", "column"),
                    FunctionCall(
                        "alias2",
                        "f1",
                        (
                            Column(None, None, "not_column2"),
                            Column(None, None, "column3"),
                        ),
                    ),
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
                ],
            )
        ),
        TranslationMappers(
            columns=[SimpleColumnMapper(None, "column2", None, "not_column2")],
            subscriptables=[TagMapper(None, "tags", None, "tags")],
        ),
    ),
]


@pytest.mark.parametrize("name, query, expected, mappers", test_cases)
def test_translation(
    name: str, query: SnubaQuery, expected: ClickhouseQuery, mappers: TranslationMappers
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
