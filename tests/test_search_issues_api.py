import uuid
from datetime import datetime, timedelta
from typing import Any, Callable

import simplejson as json

from snuba.core.initialize import initialize_snuba
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.datasets.configuration.utils import ConfigurationTest
from tests.helpers import write_raw_unprocessed_events


class TestSearchIssuesSnQLApi(BaseApiTest, ConfigurationTest):
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

        evt = dict(
            organization_id=1,
            project_id=2,
            group_ids=[3],
            data={},
            occurrence_data=dict(
                id=str(uuid.uuid4()),
                type=1,
                issue_title="search me",
                fingerprint=["one", "two"],
                detection_time=now.isoformat(),
            ),
            retention_days=90,
        )

        assert self.events_storage
        write_raw_unprocessed_events(self.events_storage, [[1, "insert", evt]])

        response = self.post_query(
            f"""MATCH (search_issues)
                                    SELECT count() AS count BY project_id
                                    WHERE project_id = {evt["project_id"]}
                                    AND detection_timestamp >= toDateTime('{(now - timedelta(minutes=1)).isoformat()}')
                                    AND detection_timestamp < toDateTime('{(now + timedelta(minutes=1)).isoformat()}')
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
