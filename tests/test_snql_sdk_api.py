import uuid
from datetime import datetime, timedelta
from typing import Any, Callable

import simplejson as json
from snuba_sdk import (
    Column,
    Condition,
    Direction,
    Entity,
    Function,
    Identifier,
    Join,
    Lambda,
    Op,
    OrderBy,
    Query,
    Relationship,
)

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestSDKSnQLApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.org_id = self.event["organization_id"]
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=180)
        events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        assert events_storage is not None
        write_unprocessed_events(events_storage, [self.event])
        self.next_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=180)
        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS),
            [get_raw_transaction()],
        )

    def test_simple_query(self) -> None:
        query = (
            Query("discover", Entity("discover_events"))
            .set_select([Function("count", [], "count")])
            .set_groupby([Column("project_id"), Column("tags[custom_tag]")])
            .set_where(
                [
                    Condition(Column("type"), Op.NEQ, "transaction"),
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                ]
            )
            .set_orderby([OrderBy(Function("count", [], "count"), Direction.ASC)])
            .set_limit(1000)
            .set_consistent(True)
            .set_debug(True)
        )

        response = self.post("/discover/snql", data=query.snuba())
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {
                "count": 1,
                "tags[custom_tag]": "custom_value",
                "project_id": self.project_id,
            }
        ]

    def test_sessions_query(self) -> None:
        query = (
            Query("sessions", Entity("sessions"))
            .set_select([Column("project_id"), Column("release")])
            .set_groupby([Column("project_id"), Column("release")])
            .set_where(
                [
                    Condition(Column("project_id"), Op.IN, [self.project_id]),
                    Condition(Column("org_id"), Op.EQ, self.org_id),
                    Condition(
                        Column("started"),
                        Op.GTE,
                        datetime(2021, 1, 1, 17, 5, 59, 554860),
                    ),
                    Condition(
                        Column("started"), Op.LT, datetime(2022, 1, 1, 17, 6, 0, 554981)
                    ),
                ]
            )
            .set_orderby([OrderBy(Column("sessions"), Direction.DESC)])
            .set_limit(100)
        )

        response = self.post("/sessions/snql", data=query.snuba())
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_join_query(self) -> None:
        ev = Entity("events", "ev")
        gm = Entity("groupedmessage", "gm")
        join = Join([Relationship(ev, "grouped", gm)])
        query = (
            Query("discover", join)
            .set_select(
                [
                    Column("group_id", ev),
                    Column("status", gm),
                    Function("avg", [Column("retention_days", ev)], "avg"),
                ]
            )
            .set_groupby([Column("group_id", ev), Column("status", gm)])
            .set_where(
                [
                    Condition(Column("project_id", ev), Op.EQ, self.project_id),
                    Condition(Column("project_id", gm), Op.EQ, self.project_id),
                    Condition(Column("timestamp", ev), Op.GTE, self.base_time),
                    Condition(Column("timestamp", ev), Op.LT, self.next_time),
                ]
            )
        )

        response = self.post("/discover/snql", data=query.snuba())
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_sub_query(self) -> None:
        inner_query = (
            Query("discover", Entity("discover_events"))
            .set_select([Function("count", [], "count")])
            .set_groupby([Column("project_id"), Column("tags[custom_tag]")])
            .set_where(
                [
                    Condition(Column("type"), Op.NEQ, "transaction"),
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                ]
            )
        )

        query = (
            Query("discover", inner_query)
            .set_select([Function("avg", [Column("count")], "avg_count")])
            .set_orderby(
                [
                    OrderBy(
                        Function("avg", [Column("count")], "avg_count"), Direction.ASC
                    )
                ]
            )
            .set_limit(1000)
        )

        response = self.post("/discover/snql", data=query.snuba())
        data = json.loads(response.data)
        assert response.status_code == 200, data
        assert data["data"] == [{"avg_count": 1.0}]

    def test_arrayjoin(self) -> None:
        query = (
            Query("events", Entity("events"))
            .set_select(
                [
                    Function("count", [], "times_seen"),
                    Function("min", [Column("timestamp")], "first_seen"),
                    Function("max", [Column("timestamp")], "last_seen"),
                ]
            )
            .set_groupby([Column("exception_frames.filename")])
            .set_array_join([Column("exception_frames.filename")])
            .set_where(
                [
                    Condition(Column("exception_frames.filename"), Op.LIKE, "%.java"),
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                ]
            )
            .set_orderby(
                [
                    OrderBy(
                        Function("max", [Column("timestamp")], "last_seen"),
                        Direction.DESC,
                    )
                ]
            )
            .set_limit(1000)
        )

        response = self.post("/events/snql", data=query.snuba())
        data = json.loads(response.data)
        assert response.status_code == 200, data
        assert len(data["data"]) == 6

    def test_tags_in_groupby(self) -> None:
        query = (
            Query("events", Entity("events"))
            .set_select(
                [
                    Function("count", [], "times_seen"),
                    Function("min", [Column("timestamp")], "first_seen"),
                    Function("max", [Column("timestamp")], "last_seen"),
                ]
            )
            .set_groupby([Column("tags[k8s-app]")])
            .set_where(
                [
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                    Condition(Column("tags[k8s-app]"), Op.NEQ, ""),
                    Condition(Column("type"), Op.NEQ, "transaction"),
                ]
            )
            .set_orderby(
                [
                    OrderBy(
                        Function("max", [Column("timestamp")], "last_seen"),
                        Direction.DESC,
                    )
                ]
            )
            .set_limit(1000)
        )

        response = self.post("/events/snql", data=query.snuba())
        data = json.loads(response.data)
        assert response.status_code == 200, data

    def test_array_condition_unpack_in_join_query(self) -> None:
        ev = Entity("events", "ev")
        gm = Entity("groupedmessage", "gm")
        join = Join([Relationship(ev, "grouped", gm)])
        query = (
            Query("discover", join)
            .set_select(
                [
                    Column("group_id", ev),
                    Column("status", gm),
                    Function("avg", [Column("retention_days", ev)], "avg"),
                ]
            )
            .set_groupby([Column("group_id", ev), Column("status", gm)])
            .set_where(
                [
                    Condition(Column("project_id", ev), Op.EQ, self.project_id),
                    Condition(Column("project_id", gm), Op.EQ, self.project_id),
                    Condition(Column("timestamp", ev), Op.GTE, self.base_time),
                    Condition(Column("timestamp", ev), Op.LT, self.next_time),
                    Condition(
                        Column("exception_stacks.type", ev), Op.LIKE, "Arithmetic%"
                    ),
                ]
            )
            .set_debug(True)
        )

        response = self.post("/discover/snql", data=query.snuba())
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_escape_edge_cases(self) -> None:
        query = (
            Query("events", Entity("events"))
            .set_select([Function("count", [], "times_seen")])
            .set_where(
                [
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                    Condition(Column("environment"), Op.EQ, "\\' \n \\n \\"),
                ]
            )
        )

        response = self.post("/events/snql", data=query.snuba())
        data = json.loads(response.data)
        assert response.status_code == 200, data

    def test_suspect_spans_lambdas(self) -> None:
        query = (
            Query("discover", Entity("discover_transactions"))
            .set_select(
                [
                    Column("spans.op"),
                    Column("spans.group"),
                    Function(
                        "arrayReduce",
                        [
                            "sumIf",
                            Column("spans.exclusive_time_32"),
                            Function(
                                "arrayMap",
                                [
                                    Lambda(
                                        ["x", "y"],
                                        Function(
                                            "if",
                                            [
                                                Function(
                                                    "equals",
                                                    [
                                                        Function(
                                                            "and",
                                                            [
                                                                Function(
                                                                    "equals",
                                                                    [
                                                                        Identifier("x"),
                                                                        "db",
                                                                    ],
                                                                ),
                                                                Function(
                                                                    "equals",
                                                                    [
                                                                        Identifier("y"),
                                                                        "05029609156d8133",
                                                                    ],
                                                                ),
                                                            ],
                                                        ),
                                                        1,
                                                    ],
                                                ),
                                                1,
                                                0,
                                            ],
                                        ),
                                    ),
                                    Column("spans.op"),
                                    Column("spans.group"),
                                ],
                            ),
                        ],
                        "array_spans_exclusive_time",
                    ),
                ]
            )
            .set_where(
                [
                    Condition(Column("transaction_name"), Op.EQ, "/api/do_things"),
                    Condition(Function("has", [Column("spans.op"), "db"]), Op.EQ, 1),
                    Condition(
                        Function("has", [Column("spans.group"), "05029609156d8133"]),
                        Op.EQ,
                        1,
                    ),
                    Condition(Column("duration"), Op.LT, 900000.0),
                    Condition(Column("finish_ts"), Op.GTE, self.base_time),
                    Condition(Column("finish_ts"), Op.LT, self.next_time),
                    Condition(Column("project_id"), Op.IN, (self.project_id,)),
                ]
            )
            .set_orderby(
                [OrderBy(Column("array_spans_exclusive_time"), Direction.DESC)]
            )
            .set_limit(10)
        )

        response = self.post("/discover/snql", data=query.snuba())
        resp = json.loads(response.data)
        assert response.status_code == 200, resp
        data = resp["data"]
        assert len(data) == 1
        assert data[0]["array_spans_exclusive_time"] > 0

    def test_attribution_tags(self) -> None:
        query = (
            Query("events", Entity("events"))
            .set_select([Function("count", [], "count")])
            .set_where(
                [
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                ]
            )
            .set_team("sns")
            .set_feature("test")
        )

        response = self.post("/events/snql", data=query.snuba())
        resp = json.loads(response.data)
        assert response.status_code == 200, resp
        metric_calls = get_recorded_metric_calls("increment", "snuba.attribution.log")
        assert metric_calls is not None
        assert len(metric_calls) == 1
        assert metric_calls[0].value > 0
        assert metric_calls[0].tags["app_id"] == "default"

    def test_invalid_time_conditions(self) -> None:
        query = (
            Query("events", Entity("events"))
            .set_select([Function("count", [], "count")])
            .set_where(
                [
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.next_time),
                    Condition(Column("timestamp"), Op.LT, self.base_time),
                ]
            )
        )

        response = self.post("/events/snql", data=query.snuba())
        resp = json.loads(response.data)
        assert response.status_code == 400, resp

    def test_tags_raw_access(self) -> None:
        query = (
            Query("generic_metrics", Entity("generic_metrics_distributions"))
            .set_select([Function("count", [], "count")])
            .set_where(
                conditions=[
                    Condition(Column("tags_raw[1234]"), Op.EQ, "condition-value"),
                    Condition(Column("org_id"), Op.EQ, self.org_id),
                    Condition(Column("project_id"), Op.EQ, self.project_id),
                    Condition(Column("timestamp"), Op.GTE, self.base_time),
                    Condition(Column("timestamp"), Op.LT, self.next_time),
                ]
            )
        )

        response = self.post("/generic_metrics/snql", data=query.snuba())
        resp = json.loads(response.data)
        assert response.status_code == 200, resp
