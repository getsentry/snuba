from copy import deepcopy

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.physical.uniq_in_select_and_having import (
    MismatchedAggregationException,
    UniqInSelectAndHavingProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query


def uniq_expression(alias: str = None, column_name: str = "user") -> FunctionCall:
    return FunctionCall(
        None,
        "greater",
        (
            FunctionCall(alias, "uniq", (Column(None, None, column_name),)),
            Literal(None, 1),
        ),
    )


INVALID_QUERY_CASES = [
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
            ],
            condition=None,
            having=uniq_expression(),
        ),
        id="uniq in having, not in select",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
                uniq_expression(alias="my_alias", column_name="some_column"),
            ],
            condition=None,
            having=uniq_expression(alias="my_alias", column_name="some_other_column"),
        ),
        id="same aggregation, different columns",
    ),
]

VALID_QUERY_CASES = [
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
                # the parser guarantees alias expansion
                uniq_expression(alias="my_alias"),
            ],
            condition=None,
            having=uniq_expression(alias="my_alias"),
        ),
        id="same alias",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                Column("_snuba_project_id", None, "project_id"),
                Column(None, None, "transaction_name"),
                uniq_expression(alias="different_alias"),
            ],
            condition=None,
            having=uniq_expression(alias="my_alias"),
        ),
        id="same aggregation, different alias",
    ),
]


@pytest.mark.parametrize("input_query", deepcopy(INVALID_QUERY_CASES))
@pytest.mark.redis_db
def test_invalid_uniq_queries(input_query: ClickhouseQuery) -> None:
    set_config("throw_on_uniq_select_and_having", True)
    with pytest.raises(MismatchedAggregationException):
        UniqInSelectAndHavingProcessor().process_query(input_query, HTTPQuerySettings())


@pytest.mark.parametrize("input_query", deepcopy(VALID_QUERY_CASES))
@pytest.mark.redis_db
def test_valid_uniq_queries(input_query: ClickhouseQuery) -> None:
    set_config("throw_on_uniq_select_and_having", True)
    og_query = deepcopy(input_query)
    UniqInSelectAndHavingProcessor().process_query(input_query, HTTPQuerySettings())
    # query should not change
    assert og_query == input_query
