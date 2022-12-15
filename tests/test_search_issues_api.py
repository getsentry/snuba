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
                }
            ),
            headers={"referer": "test"},
        )

    def test_simple_search_query(self) -> None:
        now = datetime.now().replace(minute=0, second=0, microsecond=0)

        evt: MutableMapping[str, Any] = dict(
            organization_id=1,
            project_id=2,
            group_ids=[3],
            primary_hash=str(uuid.uuid4().hex),
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

    def test_test_evenstream_endpoint(self) -> None:
        now = datetime.now()

        event = (
            2,
            "insert",
            {
                "project_id": 1,
                "organization_id": 2,
                "group_ids": [3],
                "retention_days": 90,
                "primary_hash": str(uuid.uuid4()),
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
        response = self.app.post(
            "/tests/search_issues/eventstream", data=json.dumps(event)
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

        # the below uses the snuba_sdk which currently doesn't map "from_date" and "to_date"
        # query params to new dataset timestamp columns
        #
        # query = {
        #     "project": 1,
        #     "selected_columns": [],
        #     "groupby": "project_id",
        #     "aggregations": [["count()", "", "count"]],
        #     "conditions": [["group_id", "=", 3],
        #                    # ["detection_timestamp", ">=", (now - timedelta(minutes=1)).timestamp()],
        #                    # ["detection_timestamp", "<", (now + timedelta(minutes=1)).timestamp()]
        #                    ],
        #     "from_date": (now - timedelta(days=1)).isoformat(),
        #     "to_date": (now + timedelta(days=1)).isoformat(),
        # }
        # response = self.post(json.dumps(query))
        # result = json.loads(response.data)
        # assert result["data"] == [{"count": 1, "project_id": 1}]
