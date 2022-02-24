from copy import deepcopy

import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.uniq_killswitch import UniqKillswitchProcessor
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        Query(
            Table("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "uniq", (Column(None, None, "column1"),)),
                ),
                SelectedExpression(
                    "alias2",
                    FunctionCall("alias2", "count", (Column(None, None, "column2"),)),
                ),
            ],
        ),
        Query(
            Table("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("alias", Literal("alias", 0),),
                SelectedExpression(
                    "alias2",
                    FunctionCall("alias2", "count", (Column(None, None, "column2"),)),
                ),
            ],
        ),
    ),
]


@pytest.mark.parametrize("input_query, expected_query", test_data)
def test_kill_uniq_processor(input_query: Query, expected_query: Query) -> None:
    # Matching referrer
    state.set_config("uniq_killswitch_referrers", "test,test2")
    copy = deepcopy(input_query)
    UniqKillswitchProcessor().process_query(copy, HTTPRequestSettings(referrer="test"))
    assert copy == expected_query
    assert copy != input_query

    # Non matching referrer
    state.set_config("uniq_killswitch_referrers", "test,test2")
    copy = deepcopy(input_query)
    UniqKillswitchProcessor().process_query(
        copy, HTTPRequestSettings(referrer="something_else")
    )
    assert copy != expected_query
    assert copy == input_query

    # Set tables
    state.set_config("uniq_killswitch_tables", "events")
    copy = deepcopy(input_query)
    UniqKillswitchProcessor().process_query(copy, HTTPRequestSettings())
    assert copy == expected_query
