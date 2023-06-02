import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, MutableMapping, Tuple, Union

import pytest
import simplejson as json

from snuba.core.initialize import initialize_snuba
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.datasets.configuration.utils import ConfigurationTest
from tests.helpers import write_unprocessed_events
from tests.test_api import SimpleAPITest


def base_insert_event(
    now: datetime = datetime.now(),
) -> Tuple[int, str, MutableMapping[str, Any]]:
    return (
        2,
        "insert",
        {
            "project_id": 1,
            "organization_id": 2,
            "event_id": str(uuid.uuid4()),
            "group_id": 3,
            "retention_days": 90,
            "primary_hash": str(uuid.uuid4()),
            "datetime": datetime.utcnow().isoformat() + "Z",
            "platform": "other",
            "data": {
                "received": now.timestamp(),
            },
            "occurrence_data": {
                "id": str(uuid.uuid4()),
                "type": 1,
                "issue_title": "search me",
                "fingerprint": ["one", "two"],
                "detection_time": now.timestamp(),
            },
        },
    )


class TestSearchIssuesSnQLApi(SimpleAPITest, BaseApiTest, ConfigurationTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "search_issues"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        initialize_snuba()
        self.events_storage = get_entity(EntityKey.SEARCH_ISSUES).get_writable_storage()
        assert self.events_storage is not None

    def post_query(
        self,
        query: str,
        turbo: bool = False,
        consistent: bool = True,
        debug: bool = True,
    ) -> Any:
        return self.app.post(
            "/search_issues/snql",
            data=json.dumps(
                {
                    "query": query,
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "test", "organization_id": 1},
                }
            ),
            headers={"referer": "test"},
        )

    def test_simple_search_query(self) -> None:
        now = datetime.now().replace(minute=0, second=0, microsecond=0)

        evt: MutableMapping[str, Any] = dict(
            organization_id=1,
            project_id=2,
            event_id=str(uuid.uuid4().hex),
            group_id=3,
            primary_hash=str(uuid.uuid4().hex),
            datetime=datetime.utcnow().isoformat() + "Z",
            platform="other",
            data={"received": now.timestamp()},
            occurrence_data=dict(
                id=str(uuid.uuid4().hex),
                type=1,
                issue_title="search me",
                fingerprint=["one", "two"],
                detection_time=now.timestamp(),
            ),
            retention_days=90,
        )

        assert self.events_storage
        write_unprocessed_events(self.events_storage, [evt])

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()

        response = self.post_query(
            f"""MATCH (search_issues)
                SELECT count() AS count BY project_id
                WHERE project_id = {evt["project_id"]}
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                LIMIT 1000
            """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {
                "project_id": 2,
                "count": 1,
            }
        ]

    def test_eventstream_endpoint(self) -> None:
        now = datetime.now()
        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(base_insert_event(now))
        )
        assert response.status_code == 200

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()
        response = self.post_query(
            f"""MATCH (search_issues)
                SELECT count() AS count BY project_id
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                LIMIT 1000
            """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {
                "project_id": 1,
                "count": 1,
            }
        ]

    def test_eventstream_query_optional_columns(self) -> None:
        now = datetime.now()

        insert_row = base_insert_event(now)
        insert_row[2]["occurrence_data"]["resource_id"] = uuid.uuid4().hex
        insert_row[2]["occurrence_data"]["subtitle"] = "my subtitle"
        insert_row[2]["occurrence_data"]["culprit"] = "my culprit"
        insert_row[2]["occurrence_data"]["level"] = "info"

        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(insert_row)
        )
        assert response.status_code == 200

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()
        response = self.post_query(
            f"""MATCH (search_issues)
                SELECT project_id, event_id, resource_id, subtitle, culprit, level
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
            """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {
                "project_id": 1,
                "event_id": insert_row[2]["event_id"].replace("-", ""),
                "resource_id": insert_row[2]["occurrence_data"]["resource_id"],
                "subtitle": insert_row[2]["occurrence_data"]["subtitle"],
                "culprit": insert_row[2]["occurrence_data"]["culprit"],
                "level": insert_row[2]["occurrence_data"]["level"],
            }
        ]

    def test_eventstream_query_transaction_duration(self) -> None:
        now = datetime.utcnow()
        insert_row = base_insert_event(now)
        insert_row[2]["data"]["start_timestamp"] = int(
            (now - timedelta(seconds=10)).timestamp()
        )
        insert_row[2]["data"]["timestamp"] = int(now.timestamp())

        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(insert_row)
        )
        assert response.status_code == 200

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()
        response = self.post_query(
            f"""MATCH (search_issues)
                SELECT project_id, transaction_duration
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
            """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [{"project_id": 1, "transaction_duration": 10000}]

    def test_eventstream_query_transaction_maps_to_tags(self) -> None:
        transaction_name = "/api/im/the/best"
        now = datetime.utcnow()
        insert_row = base_insert_event(now)
        insert_row[2]["data"]["tags"] = {"transaction": transaction_name}

        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(insert_row)
        )
        assert response.status_code == 200

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()
        for alias in ["transaction", "transaction_name"]:
            response = self.post_query(
                f"""MATCH (search_issues)
                    SELECT project_id, {alias}
                    WHERE project_id = 1
                    AND timestamp >= toDateTime('{from_date}')
                    AND timestamp < toDateTime('{to_date}')
                """
            )

            data = json.loads(response.data)

            assert response.status_code == 200, data
            assert data["stats"]["consistent"]
            assert data["data"] == [{"project_id": 1, f"{alias}": transaction_name}]

    def test_eventstream_query_profile_id_replay_id(self) -> None:
        profile_id = str(uuid.uuid4())
        replay_id = str(uuid.uuid4())
        now = datetime.utcnow()
        insert_row = base_insert_event(now)
        insert_row[2]["data"]["contexts"] = {
            "profile": {"profile_id": profile_id},
            "replay": {"replay_id": replay_id},
        }

        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(insert_row)
        )
        assert response.status_code == 200

        from_date = (now - timedelta(days=1)).isoformat()
        to_date = (now + timedelta(days=1)).isoformat()
        response = self.post_query(
            f"""MATCH (search_issues)
                        SELECT project_id, profile_id, replay_id
                        WHERE project_id = 1
                        AND timestamp >= toDateTime('{from_date}')
                        AND timestamp < toDateTime('{to_date}')
                    """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {"project_id": 1, "profile_id": profile_id, "replay_id": replay_id}
        ]
