from __future__ import annotations

from collections.abc import Mapping

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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {
            "_snuba_event_id": ["event_id"],
            "_snuba_duration": ["duration"],
            "_snuba_message": ["message"],
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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {
            "_snuba_event_id": ["event_id"],
            "_snuba_duration": ["duration"],
            "_snuba_message": ["message"],
        },
        id="Result with final, complete mapping",
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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
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
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {"_snuba_event_id": ["event_id"]},
        id="Incomplete mapping",
    ),
    pytest.param(
        QueryResult(
            result=Result(
                meta=[
                    Column(name="event_id", type="String"),
                    Column(name="duration", type="UInt32"),
                ],
                data=[
                    {"event_id": "asd", "duration": 123},
                    {"event_id": "sdf", "duration": 321},
                ],
                totals={"event_id": "", "duration": 223},
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="event_id", type="String"),
                    Column(name="duration", type="UInt32"),
                ],
                data=[
                    {"event_id": "asd", "duration": 123},
                    {"event_id": "sdf", "duration": 321},
                ],
                totals={"event_id": "", "duration": 223},
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {"event_id": ["event_id"], "duration": ["duration"]},
        id="Identity mapping leaves rows untouched",
    ),
    pytest.param(
        QueryResult(
            result=Result(
                meta=[Column(name="_snuba_duration", type="UInt32")],
                data=[{"_snuba_duration": 123}, {"_snuba_duration": 321}],
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="duration", type="UInt32"),
                    Column(name="duration_ms", type="UInt32"),
                ],
                data=[
                    {"duration": 123, "duration_ms": 123},
                    {"duration": 321, "duration_ms": 321},
                ],
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {"_snuba_duration": ["duration", "duration_ms"]},
        id="Fan-out: one alias to multiple names",
    ),
    pytest.param(
        # A rename target that collides with an existing column name forces the
        # new-dict fallback. The behavior (last write wins, by row iteration
        # order) must match the original implementation.
        QueryResult(
            result=Result(
                meta=[
                    Column(name="x", type="String"),
                    Column(name="_snuba_a", type="String"),
                ],
                data=[{"x": "vx", "_snuba_a": "va"}],
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        QueryResult(
            result=Result(
                meta=[
                    Column(name="x", type="String"),
                    Column(name="x", type="String"),
                ],
                data=[{"x": "va"}],
            ),
            extra=QueryExtraData(stats={}, sql="...", experiments={}),
        ),
        {"_snuba_a": ["x"]},
        id="Target collides with existing column (fallback)",
    ),
]


@pytest.mark.parametrize("in_result, out_result, mapping", TEST_CASES)
def test_transformation(
    in_result: QueryResult, out_result: QueryResult, mapping: Mapping[str, list[str]]
) -> None:
    transform_column_names(in_result, mapping)

    assert in_result == out_result
