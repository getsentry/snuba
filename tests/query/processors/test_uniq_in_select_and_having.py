from copy import deepcopy

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.uniq_in_select_and_having import (
    MismatchedAggregationException,
    UniqInSelectAndHavingProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings
from tests.query.processors.query_builders import build_query


def uniq_user_expression(alias: str = None) -> FunctionCall:
    return FunctionCall(
        None,
        "greater",
        (FunctionCall(alias, "uniq", (Column(None, None, "user"),)), Literal(None, 1)),
    )


INVALID_QUERY_CASES = [
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
            ],
            condition=None,
            having=uniq_user_expression(),
        ),
        id="prod issue repro",
    ),
]

VALID_QUERY_CASES = [
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
                Column(None, None, "my_alias"),
            ],
            condition=None,
            having=uniq_user_expression(alias="my_alias"),
        ),
        id="alias in the select",
    )
]


@pytest.mark.parametrize("input_query", deepcopy(INVALID_QUERY_CASES))
def test_invalid_uniq_queries(input_query: ClickhouseQuery) -> None:
    with pytest.raises(MismatchedAggregationException):
        UniqInSelectAndHavingProcessor().process_query(
            input_query, HTTPRequestSettings()
        )


@pytest.mark.parametrize("input_query", deepcopy(VALID_QUERY_CASES))
def test_valid_uniq_queries(input_query: ClickhouseQuery) -> None:
    og_query = deepcopy(input_query)
    UniqInSelectAndHavingProcessor().process_query(input_query, HTTPRequestSettings())
    # query should not change
    assert og_query == input_query
