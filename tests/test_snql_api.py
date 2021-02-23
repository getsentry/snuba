import uuid
import simplejson as json
from datetime import datetime, timedelta
from functools import partial

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestSnQLApi(BaseApiTest):
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
        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover_events )
                    SELECT count() AS count BY project_id, tags[custom_tag]
                    WHERE type != 'transaction' AND project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    ORDER BY count ASC
                    LIMIT 1000""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                }
            ),
        )
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
        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "dataset": "sessions",
                    "query": f"""MATCH (sessions)
                    SELECT project_id, release BY release, project_id
                    WHERE project_id IN array({self.project_id})
                    AND project_id IN array({self.project_id})
                    AND org_id = {self.org_id}
                    AND started >= toDateTime('2021-01-01T17:05:59.554860')
                    AND started < toDateTime('2022-01-01T17:06:00.554981')
                    ORDER BY sessions DESC
                    LIMIT 100 OFFSET 0""",
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_join_query(self) -> None:
        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (s: spans) -[contained]-> (t: transactions)
                    SELECT s.op, avg(s.duration_ms) AS avg BY s.op
                    WHERE s.project_id = {self.project_id}
                    AND t.project_id = {self.project_id}
                    AND t.finish_ts >= toDateTime('2021-01-01')
                    AND t.finish_ts < toDateTime('2021-01-02')
                    """,
                    "turbo": False,
                    "consistent": False,
                    "debug": True,
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_sub_query(self) -> None:
        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": """MATCH {
                        MATCH (discover_events )
                        SELECT count() AS count BY project_id, tags[custom_tag]
                        WHERE type != 'transaction' AND project_id = %s
                        AND timestamp >= toDateTime('%s')
                        AND timestamp < toDateTime('%s')
                    }
                    SELECT avg(count) AS avg_count
                    ORDER BY avg_count ASC
                    LIMIT 1000"""
                    % (
                        self.project_id,
                        self.base_time.isoformat(),
                        self.next_time.isoformat(),
                    ),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["data"] == [{"avg_count": 1.0}]

    def test_project_rate_limiting(self) -> None:
        state.set_config("project_concurrent_limit", self.project_id)
        state.set_config(f"project_concurrent_limit_{self.project_id}", 0)

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": """MATCH (events)
                    SELECT platform
                    WHERE project_id = 2
                    AND timestamp >= toDateTime('2021-01-01')
                    AND timestamp < toDateTime('2021-01-02')
                    """,
                }
            ),
        )
        assert response.status_code == 200

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT platform
                    WHERE project_id = {self.project_id}
                    AND timestamp >= toDateTime('2021-01-01')
                    AND timestamp < toDateTime('2021-01-02')
                    """
                }
            ),
        )
        assert response.status_code == 429

    def test_project_rate_limiting_joins(self) -> None:
        state.set_config("project_concurrent_limit", self.project_id)
        state.set_config(f"project_concurrent_limit_{self.project_id}", 0)

        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": """MATCH (s: spans) -[contained]-> (t: transactions)
                    SELECT s.op, avg(s.duration_ms) AS avg BY s.op
                    WHERE s.project_id = 2
                    AND t.project_id = 2
                    AND t.finish_ts >= toDateTime('2021-01-01')
                    AND t.finish_ts < toDateTime('2021-01-02')
                    """,
                }
            ),
        )
        assert response.status_code == 200

        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (s: spans) -[contained]-> (t: transactions)
                    SELECT s.op, avg(s.duration_ms) AS avg BY s.op
                    WHERE s.project_id = {self.project_id}
                    AND t.project_id = {self.project_id}
                    AND t.finish_ts >= toDateTime('2021-01-01')
                    AND t.finish_ts < toDateTime('2021-01-02')
                    """,
                }
            ),
        )
        assert response.status_code == 429

    def test_project_rate_limiting_subqueries(self) -> None:
        state.set_config("project_concurrent_limit", self.project_id)
        state.set_config(f"project_concurrent_limit_{self.project_id}", 0)

        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": """MATCH {
                        MATCH (discover_events )
                        SELECT count() AS count BY project_id, tags[custom_tag]
                        WHERE type != 'transaction' AND project_id = 2
                        AND timestamp >= toDateTime('%s')
                        AND timestamp < toDateTime('%s')
                    }
                    SELECT avg(count) AS avg_count
                    ORDER BY avg_count ASC
                    LIMIT 1000"""
                    % (self.base_time.isoformat(), self.next_time.isoformat()),
                }
            ),
        )
        assert response.status_code == 200

        response = self.app.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": """MATCH {
                        MATCH (discover_events )
                        SELECT count() AS count BY project_id, tags[custom_tag]
                        WHERE type != 'transaction' AND project_id = %s
                        AND timestamp >= toDateTime('%s')
                        AND timestamp < toDateTime('%s')
                    }
                    SELECT avg(count) AS avg_count
                    ORDER BY avg_count ASC
                    LIMIT 1000"""
                    % (
                        self.project_id,
                        self.base_time.isoformat(),
                        self.next_time.isoformat(),
                    ),
                }
            ),
        )
        assert response.status_code == 429
