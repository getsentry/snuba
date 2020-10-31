from typing import Mapping

import pytest
from snuba.reader import Column, Result
from snuba.web import QueryExtraData, QueryResult, transform_column_names

TEST_CASES = [
    pytest.param(
        QueryResult(
            result=Result(
                meta=[
                    Column(name="_snuba_event_id", type="String"),
                    Column(name="_snuba_duration", type="UInt32"),
                    Column(name="_snuba_message", type="String"),
                ],
                data=[
                    {
                        "_snuba_event_id": "asd",
                        "_snuba_duration": 123,
                        "_snuba_message": "msg",
                    },
                    {
                        "_snuba_event_id": "sdf",
                        "_snuba_duration": 321,
                        "_snuba_message": "msg2",
                    },
                ],
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="event_id", type="String"),
                    Column(name="duration", type="UInt32"),
                    Column(name="message", type="String"),
                ],
                data=[
                    {"event_id": "asd", "duration": 123, "message": "msg"},
                    {"event_id": "sdf", "duration": 321, "message": "msg2"},
                ],
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        {
            "_snuba_event_id": "event_id",
            "_snuba_duration": "duration",
            "_snuba_message": "message",
        },
        id="Result without final, complete mapping",
    ),
    pytest.param(
        QueryResult(
            result=Result(
                meta=[
                    Column(name="_snuba_event_id", type="String"),
                    Column(name="_snuba_duration", type="UInt32"),
                    Column(name="_snuba_message", type="String"),
                ],
                data=[
                    {
                        "_snuba_event_id": "asd",
                        "_snuba_duration": 123,
                        "_snuba_message": "msg",
                    },
                    {
                        "_snuba_event_id": "sdf",
                        "_snuba_duration": 321,
                        "_snuba_message": "msg2",
                    },
                ],
                totals={
                    "_snuba_event_id": "",
                    "_snuba_duration": 223,
                    "_snuba_message": "",
                },
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="event_id", type="String"),
                    Column(name="duration", type="UInt32"),
                    Column(name="message", type="String"),
                ],
                data=[
                    {"event_id": "asd", "duration": 123, "message": "msg"},
                    {"event_id": "sdf", "duration": 321, "message": "msg2"},
                ],
                totals={"event_id": "", "duration": 223, "message": ""},
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        {
            "_snuba_event_id": "event_id",
            "_snuba_duration": "duration",
            "_snuba_message": "message",
        },
        id="Result without final, complete mapping",
    ),
    pytest.param(
        QueryResult(
            result=Result(
                meta=[
                    Column(name="_snuba_event_id", type="String"),
                    Column(name="_snuba_duration", type="UInt32"),
                    Column(name="_snuba_message", type="String"),
                ],
                data=[
                    {
                        "_snuba_event_id": "asd",
                        "_snuba_duration": 123,
                        "_snuba_message": "msg",
                    },
                    {
                        "_snuba_event_id": "sdf",
                        "_snuba_duration": 321,
                        "_snuba_message": "msg2",
                    },
                ],
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="event_id", type="String"),
                    Column(name="_snuba_duration", type="UInt32"),
                    Column(name="_snuba_message", type="String"),
                ],
                data=[
                    {
                        "event_id": "asd",
                        "_snuba_duration": 123,
                        "_snuba_message": "msg",
                    },
                    {
                        "event_id": "sdf",
                        "_snuba_duration": 321,
                        "_snuba_message": "msg2",
                    },
                ],
            ),
            extra=QueryExtraData(stats={}, sql="..."),
        ),
        {"_snuba_event_id": "event_id"},
        id="Incomplete mapping",
    ),
]


@pytest.mark.parametrize("in_result, out_result, mapping", TEST_CASES)
def test_transformation(
    in_result: QueryResult, out_result: QueryResult, mapping: Mapping[str, str]
) -> None:
    transform_column_names(in_result, mapping)

    assert in_result == out_result
