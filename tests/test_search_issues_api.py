from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import simplejson as json

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


class TestSearchIssuesSnQLApi(BaseApiTest):
    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
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
        now = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        evt = dict(
            organization_id=1,
            project_id=2,
            detection_timestamp=now.timestamp(),
        )

        assert self.events_storage
        write_raw_unprocessed_events(self.events_storage, [evt])

        response = self.post_query(
            f"""MATCH (search_issues)
                                    SELECT count() AS count BY organization_id, project_id
                                    WHERE organization_id = {evt["organization_id"]}
                                    AND project_id = {evt["project_id"]}
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
                "organization_id": 1,
                "project_id": 2,
                "count": 1,
            }
        ]
