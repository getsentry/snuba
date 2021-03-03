import uuid
import simplejson as json
from datetime import datetime, timedelta
from functools import partial

from snuba_sdk.column import Column
from snuba_sdk.conditions import Condition, Op
from snuba_sdk.entity import Entity
from snuba_sdk.function import Function
from snuba_sdk.orderby import Direction, OrderBy
from snuba_sdk.query import Query
from snuba_sdk.relationships import Join, Relationship

from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage

from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestSDKSnQLApi(BaseApiTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.org_id = self.event["organization_id"]
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=180)
        events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        write_unprocessed_events(events_storage, [self.event])
        self.next_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=180)
        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS), [get_raw_transaction()],
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

        response = self.app.post("/discover/snql", data=query.snuba())
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

        response = self.app.post("/sessions/snql", data=query.snuba())
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_join_query(self) -> None:
        sp = Entity("spans", "s")
        tr = Entity("transactions", "tr")
        join = Join([Relationship(sp, "contained", tr)])
        query = (
            Query("discover", join)
            .set_select(
                [Column("op", sp), Function("avg", [Column("duration_ms", sp)], "avg")]
            )
            .set_groupby([Column("op", sp)])
            .set_where(
                [
                    Condition(Column("project_id", sp), Op.EQ, self.project_id),
                    Condition(Column("project_id", tr), Op.EQ, self.project_id),
                    Condition(Column("finish_ts", tr), Op.GTE, self.base_time),
                    Condition(Column("finish_ts", tr), Op.LT, self.next_time),
                ]
            )
        )

        response = self.app.post("/discover/snql", data=query.snuba())
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

        response = self.app.post("/discover/snql", data=query.snuba())
        data = json.loads(response.data)
        assert response.status_code == 200, data
        assert data["data"] == [{"avg_count": 1.0}]
