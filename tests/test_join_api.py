from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest
import simplejson as json

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events

pytest.skip(allow_module_level=True, reason="Join tests need non-production fixtures")


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSnQLJoinApi(BaseApiTest):
    """
    TODO: These tests were originally written using an in-production dataset (CDC). However
    that dataset no longer exists, so these tests can't be run. Once we have examples in the
    code base for production joins, we can adapt these tests to use them.

    """

    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None, redis_db: None) -> None:
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.org_id = self.event["organization_id"]
        self.group_id = self.event["group_id"]
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

    def test_join_query(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (e: events) -[grouped]-> (gm: groupedmessage)
                    SELECT e.group_id, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    WHERE e.project_id = {self.project_id}
                    AND gm.project_id = {self.project_id}
                    AND e.timestamp >= toDateTime('2021-01-01')
                    AND e.timestamp < toDateTime('2021-01-02')
                    """,
                    "turbo": False,
                    "consistent": False,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_multi_table_join(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (e: events) -[grouped]-> (g: groupedmessage),
                    (e: events) -[assigned]-> (a: groupassignee)
                    SELECT e.message, e.tags[b], a.user_id, g.last_seen
                    WHERE e.project_id = {self.project_id}
                    AND g.project_id = {self.project_id}
                    AND e.timestamp >= toDateTime('2021-06-04T00:00:00')
                    AND e.timestamp < toDateTime('2021-07-12T00:00:00')
                    """,
                    "turbo": False,
                    "consistent": False,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200

    def test_complex_table_join(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (e: events) -[grouped]-> (g: groupedmessage)
                    SELECT g.id, toUInt64(plus(multiply(log(count(e.group_id)), 600), multiply(toUInt64(toUInt64(max(e.timestamp))), 1000))) AS score BY g.id
                    WHERE e.project_id IN array({self.project_id}) AND e.timestamp >= toDateTime('2021-07-04T01:09:08.188427') AND e.timestamp < toDateTime('2021-08-06T01:10:09.411889') AND g.status IN array(0)
                    ORDER BY toUInt64(plus(multiply(log(count(e.group_id)), 600), multiply(toUInt64(toUInt64(max(e.timestamp))), 1000))) DESC LIMIT 101
                    """,
                    "turbo": False,
                    "consistent": False,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200

    def test_valid_columns_composite_query(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (e: events) -[grouped]-> (gm: groupedmessage)
                    SELECT e.group_id, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    WHERE e.project_id = {self.project_id}
                    AND gm.project_id = {self.project_id}
                    AND e.timestamp >= toDateTime('2021-01-01')
                    AND e.timestamp < toDateTime('2021-01-02')
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200

    MATCH = "MATCH (e: events) -[grouped]-> (gm: groupedmessage)"
    SELECT = "SELECT e.group_id, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status"
    WHERE = "WHERE e.project_id = 1 AND gm.project_id = 1"
    TIMESTAMPS = "AND e.timestamp >= toDateTime('2021-01-01') AND e.timestamp < toDateTime('2021-01-02')"

    invalid_columns_composite_query_tests = [
        pytest.param(
            f"""{MATCH}
                    SELECT e.fsdfsd, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: query column(s) fsdfsd do not exist",
            id="Invalid first Select column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.group_id, gm.fsdfsd, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity groupedmessage: query column(s) fsdfsd do not exist",
            id="Invalid second Select column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.group_id, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.fsdfsd
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity groupedmessage: query column(s) fsdfsd do not exist",
            id="Invalid By column",
        ),
        pytest.param(
            f"""{MATCH}
                    {SELECT}
                    WHERE e.project_id = 1
                    AND gm.fsdfsd = 1
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity groupedmessage: query column(s) fsdfsd do not exist",
            id="Invalid Where column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.status, gm.group_id, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: query column(s) status do not exist",
            id="Mismatched Select columns",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT uniq(toString(e.fdsdsf)), gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: query column(s) fdsdsf do not exist",
            id="Invalid nested column",
        ),
    ]

    @pytest.mark.parametrize(
        "query, response_code, error_message", invalid_columns_composite_query_tests
    )
    def test_invalid_columns_composite_query(
        self, query: str, response_code: int, error_message: str
    ) -> None:
        response = self.post("/events/snql", data=json.dumps({"query": query}))

        # TODO: when validation mode for events and groupedmessage is ERROR this should be:
        # assert response.status_code == response_code
        # assert json.loads(response.data)["error"]["message"] == error_message

        assert response.status_code == 500
