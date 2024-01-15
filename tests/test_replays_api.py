import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest
import rapidjson
import simplejson as json

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.fixtures import get_replay_event
from tests.helpers import write_raw_unprocessed_events


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestReplaysApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None) -> None:
        self.replay_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad05")
        self.event = get_replay_event(replay_id=str(self.replay_id))
        self.project_id = self.event["project_id"]
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=180)
        self.next_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=180)

    def test_default_json_encoder(self) -> None:
        replays_storage = get_entity(EntityKey.REPLAYS).get_writable_storage()
        assert replays_storage is not None
        write_raw_unprocessed_events(replays_storage, [self.event])

        response = self.post(
            "/replays/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (replays)
                    SELECT replay_id, ip_address_v4, groupUniqArrayArray(trace_ids) AS `trace_ids`
                    BY replay_id, ip_address_v4
                    WHERE project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    LIMIT 10 OFFSET 0
                    """,
                    "debug": True,
                    "tenant_ids": {"referrer": "replays", "organization_id": 1},
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, data

        assert data["data"] == [
            {
                "replay_id": "7400045b-25c4-43b8-8591-4600aa83ad05",
                "ip_address_v4": "127.0.0.1",
                "trace_ids": [
                    "8bea4461-d8b9-44f3-93c1-5a3cb1c4169a",
                    "36e980a9-c602-4cde-9f5d-089f15b83b5f",
                ],
            }
        ]

    def test_sdk_user_title_nullability(self) -> None:
        payload = rapidjson.loads(bytes(self.event["payload"]))
        assert isinstance(payload, dict)

        payload.pop("user")
        payload.pop("sdk")
        payload["tags"].pop("transaction")
        self.event["payload"] = list(json.dumps(payload).encode())  # type: ignore

        replays_storage = get_entity(EntityKey.REPLAYS).get_writable_storage()
        assert replays_storage is not None
        write_raw_unprocessed_events(replays_storage, [self.event])

        response = self.post(
            "/replays/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (replays)
                    SELECT title, user, sdk_name, sdk_version
                    WHERE project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    LIMIT 10 OFFSET 0
                    """,
                    "debug": True,
                    "tenant_ids": {"referrer": "replays", "organization_id": 1},
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, data

        assert data["data"] == [
            {
                "title": None,
                "user": "",
                "sdk_name": "",
                "sdk_version": "",
            }
        ]
