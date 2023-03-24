from datetime import datetime, timedelta
from functools import partial

import pytest
import pytz
import simplejson as json

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events

TEST_GROUP_JOIN_PARAMS = [
    pytest.param(
        "(e: events) -[grouped]-> (g: groupedmessage)",
        "=",
        1,
        id="events groups join on existing group",
    ),
    pytest.param(
        "(e: events) -[grouped]-> (g: groupedmessage)",
        "!=",
        0,
        id="events groups join on non existing group",
    ),
    pytest.param(
        "(g: groupedmessage) -[groups]-> (e: events)",
        "=",
        1,
        id="groups events join on existing group",
    ),
    pytest.param(
        "(g: groupedmessage) -[groups]-> (e: events)",
        "!=",
        0,
        id="groups events join on non existing group",
    ),
]

TEST_ASSIGNEE_JOIN_PARAMS = [
    pytest.param(
        "(e: events) -[assigned]-> (a: groupassignee)",
        "=",
        1,
        id="events assignees join on existing user",
    ),
    pytest.param(
        "(a: groupassignee) -[owns]-> (e: events)",
        "=",
        1,
        id="assignees events join on existing user",
    ),
]


class TestCdcEvents(BaseApiTest):
    @pytest.fixture(autouse=True)
    def setup_fixture(self, clickhouse_db, redis_db):
        self.app.post = partial(self.app.post, headers={"referer": "test"})
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.base_time = datetime.utcnow().replace(
            second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=90)
        self.next_time = self.base_time + timedelta(minutes=95)

        self.events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        write_unprocessed_events(self.events_storage, [self.event])

        groups = [
            {
                "offset": 0,
                "project_id": self.project_id,
                "id": self.event["group_id"],
                "record_deleted": 0,
                "status": 0,
            }
        ]

        groups_storage = get_entity(EntityKey.GROUPEDMESSAGE).get_writable_storage()
        groups_storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ).write([json.dumps(group).encode("utf-8") for group in groups])

        assignees = [
            {
                "offset": 0,
                "project_id": self.project_id,
                "group_id": self.event["group_id"],
                "record_deleted": 0,
                "user_id": 100,
            }
        ]

        assignees_storage = get_entity(EntityKey.GROUPASSIGNEE).get_writable_storage()
        assignees_storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ).write([json.dumps(assignee).encode("utf-8") for assignee in assignees])

    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    @pytest.mark.parametrize(
        "relationship, operator, expected_rows", TEST_GROUP_JOIN_PARAMS
    )
    def test_groups_join(
        self, relationship: str, operator: str, expected_rows: int
    ) -> None:
        query_template = (
            "MATCH %(relationship)s "
            "SELECT e.event_id WHERE "
            "e.project_id = %(project_id)s AND "
            "g.project_id = %(project_id)s AND "
            "g.id %(operator)s %(group_id)s AND "
            "e.timestamp >= toDateTime('%(btime)s') AND "
            "e.timestamp < toDateTime('%(ntime)s')"
        )

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "dataset": "events",
                    "query": query_template
                    % {
                        "relationship": relationship,
                        "project_id": self.project_id,
                        "operator": operator,
                        "group_id": self.event["group_id"],
                        "btime": self.base_time,
                        "ntime": self.next_time,
                    },
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == expected_rows, data

    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    @pytest.mark.parametrize(
        "relationship, operator, expected_rows", TEST_ASSIGNEE_JOIN_PARAMS
    )
    def test_assignee_join(
        self, relationship: str, operator: str, expected_rows: int
    ) -> None:
        query_template = (
            "MATCH %(relationship)s "
            "SELECT e.event_id WHERE "
            "e.project_id = %(project_id)s AND "
            "a.project_id = %(project_id)s AND "
            "a.user_id %(operator)s 100 AND "
            "e.timestamp >= toDateTime('%(btime)s') AND "
            "e.timestamp < toDateTime('%(ntime)s')"
        )

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "dataset": "events",
                    "query": query_template
                    % {
                        "relationship": relationship,
                        "project_id": self.project_id,
                        "operator": operator,
                        "group_id": self.event["group_id"],
                        "btime": self.base_time,
                        "ntime": self.next_time,
                    },
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == expected_rows, data
