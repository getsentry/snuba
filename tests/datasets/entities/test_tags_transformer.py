import pytest

from snuba.datasets.entities.metrics import TagsTypeTransformer
from snuba.query import SelectedExpression
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Literal, SubscriptableReference
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings


def build_query(tag_key: Literal) -> Query:
    return Query(
        from_clause=None,
        selected_columns=[
            SelectedExpression(
                "tags[10]",
                SubscriptableReference(
                    "_snuba_tags[10]", Column(None, None, "tags"), tag_key
                ),
            )
        ],
    )


def test_transformer() -> None:
    query = build_query(Literal(None, "10"))
    TagsTypeTransformer().process_query(query, HTTPRequestSettings())

    assert query.get_selected_columns()[0].expression == SubscriptableReference(
        "_snuba_tags[10]", Column(None, None, "tags"), Literal(None, 10)
    )


def test_broken_query() -> None:
    with pytest.raises(InvalidExpressionException):
        TagsTypeTransformer().process_query(
            build_query(Literal(None, "asdasd")), HTTPRequestSettings()
        )
