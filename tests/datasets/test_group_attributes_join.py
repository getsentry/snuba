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
        "(e: events) -[attributes]-> (g: group_attributes)",
        "=",
        1,
        id="events groups join on existing group attributes",
    ),
]

CLICKHOUSE_DEFAULT_DATETIME = "1970-01-01T00:00:00+00:00"


class TestEventsGroupAttributes(BaseApiTest):
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

        self.group_attributes = [
            {
                "deleted": False,
                "project_id": self.project_id,
                "group_id": self.event["group_id"],
                "group_status": 0,
                "group_substatus": 7,
                "group_first_seen": self._clickhouse_datetime_str(self.base_time),
                "group_num_comments": 0,
                "assignee_user_id": None,
                "assignee_team_id": None,
                "owner_suspect_commit_user_id": None,
                "owner_ownership_rule_user_id": None,
                "owner_ownership_rule_team_id": None,
                "owner_codeowners_user_id": None,
                "owner_codeowners_team_id": None,
                "message_timestamp": self._clickhouse_datetime_str(self.base_time),
                "partition": 1,
                "offset": 1,
            },
            {
                "deleted": True,
                "project_id": self.project_id,
                "group_id": self.event["group_id"],
                "group_status": 0,
                "group_substatus": 7,
                "group_first_seen": self._clickhouse_datetime_str(self.base_time),
                "group_num_comments": 0,
                "assignee_user_id": None,
                "assignee_team_id": None,
                "owner_suspect_commit_user_id": None,
                "owner_ownership_rule_user_id": None,
                "owner_ownership_rule_team_id": None,
                "owner_codeowners_user_id": None,
                "owner_codeowners_team_id": None,
                "message_timestamp": self._clickhouse_datetime_str(
                    self.base_time + timedelta(seconds=1)
                ),
                "partition": 1,
                "offset": 1,
            },
        ]

        groups_storage = get_entity(EntityKey.GROUP_ATTRIBUTES).get_writable_storage()
        groups_storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ).write([json.dumps(ga).encode("utf-8") for ga in self.group_attributes])

    def _convert_clickhouse_datetime_str(self, str: str) -> str:
        return (
            datetime.strptime(str, "%Y-%m-%d %H:%M:%S")
            .replace(microsecond=0, tzinfo=pytz.utc)
            .isoformat()
        )

    def _clickhouse_datetime_str(self, date_time: datetime) -> str:
        return date_time.strftime("%Y-%m-%d %H:%M:%S")

    @pytest.mark.clickhouse_db
    @pytest.mark.redis_db
    @pytest.mark.parametrize(
        "relationship, operator, expected_rows", TEST_GROUP_JOIN_PARAMS
    )
    def test_group_attributes_join(
        self, relationship: str, operator: str, expected_rows: int
    ) -> None:
        query_template = (
            "MATCH %(relationship)s "
            "SELECT e.event_id, g.group_id, g.group_status, g.group_substatus, "
            "g.group_first_seen, g.group_num_comments, "
            "g.assignee_user_id, g.assignee_team_id WHERE "
            "e.project_id = %(project_id)s AND "
            "g.project_id = %(project_id)s AND "
            "g.group_id %(operator)s %(group_id)s AND "
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
        assert (
            data["data"][0].items()
            == {
                "e.event_id": self.event["event_id"],
                "g.group_id": 0,  # 0 is a sentinel value indicating the left join on group_attributes returns no data
                "g.group_status": 0,
                "g.group_substatus": None,
                "g.group_first_seen": CLICKHOUSE_DEFAULT_DATETIME,
                # "g.group_first_seen": self._convert_clickhouse_datetime_str(self.group_attributes[0]["group_first_seen"]),
                "g.group_num_comments": 0,
                "g.assignee_user_id": None,
                "g.assignee_team_id": None,
            }.items()
        )
