import uuid
import simplejson as json
from datetime import datetime, timedelta
from typing import Any, Callable
from unittest.mock import patch, MagicMock

from snuba import state
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestSnQLApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.org_id = self.event["organization_id"]
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
            get_writable_storage(StorageKey.TRANSACTIONS), [get_raw_transaction()],
        )

    def test_simple_query(self) -> None:
        response = self.post(
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
        response = self.post(
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
        response = self.post(
            "/discover/snql",
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
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_sub_query(self) -> None:
        response = self.post(
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

    def test_max_limit(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover_events )
                    SELECT count() AS count BY project_id, tags[custom_tag]
                    WHERE type != 'transaction' AND project_id = {self.project_id} AND timestamp >= toDateTime('2021-01-01')
                    ORDER BY count ASC
                    LIMIT 100000""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 400, data

    def test_project_rate_limiting(self) -> None:
        state.set_config("project_concurrent_limit", self.project_id)
        state.set_config(f"project_concurrent_limit_{self.project_id}", 0)

        response = self.post(
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

        response = self.post(
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

        response = self.post(
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

        response = self.post(
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

        response = self.post(
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

        response = self.post(
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

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    def test_record_queries(self, record_query_mock: Any) -> None:
        for use_split, expected_query_count in [(0, 1), (1, 2)]:
            state.set_config("use_split", use_split)
            record_query_mock.reset_mock()
            result = json.loads(
                self.post(
                    "/events/snql",
                    data=json.dumps(
                        {
                            "query": f"""MATCH (events)
                            SELECT event_id, title, transaction, tags[a], tags[b], message, project_id
                            WHERE timestamp >= toDateTime('2021-01-01')
                            AND timestamp < toDateTime('2022-01-01')
                            AND project_id IN tuple({self.project_id})
                            LIMIT 5""",
                        }
                    ),
                ).data
            )

            assert len(result["data"]) == 1
            assert record_query_mock.call_count == 1
            metadata = record_query_mock.call_args[0][0]
            assert metadata["dataset"] == "events"
            assert metadata["request"]["referrer"] == "test"
            assert len(metadata["query_list"]) == expected_query_count

    @patch("snuba.web.query._run_query_pipeline")
    def test_error_handler(self, pipeline_mock: MagicMock) -> None:
        from rediscluster.utils import ClusterDownError  # type: ignore

        hsh = "0" * 32
        group_id = int(hsh[:16], 16)
        pipeline_mock.side_effect = ClusterDownError("stuff")
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT event_id, group_id, project_id, timestamp
                    WHERE project_id IN tuple({self.project_id})
                    AND group_id IN tuple({group_id})
                    AND  timestamp >= toDateTime('2021-01-01')
                    AND timestamp < toDateTime('2022-01-01')
                    ORDER BY timestamp DESC, event_id DESC
                    LIMIT 1""",
                }
            ),
        )
        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["error"]["type"] == "internal_server_error"
        assert data["error"]["message"] == "stuff"

    def test_sessions_with_function_orderby(self) -> None:
        response = self.post(
            "/sessions/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (sessions)
                    SELECT project_id, release BY release, project_id
                    WHERE org_id = {self.org_id}
                    AND started >= toDateTime('2021-04-05T16:52:48.907628')
                    AND started < toDateTime('2021-04-06T16:52:49.907666')
                    AND project_id IN tuple({self.project_id})
                    AND project_id IN tuple({self.project_id})
                    ORDER BY divide(sessions_crashed, sessions) ASC
                    LIMIT 21
                    OFFSET 0
                    """,
                }
            ),
        )
        assert response.status_code == 200

    def test_arrayjoin(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT count() AS times_seen, min(timestamp) AS first_seen, max(timestamp) AS last_seen
                    BY exception_frames.filename
                    ARRAY JOIN exception_frames.filename
                    WHERE timestamp >= toDateTime('2021-04-16T17:17:00')
                    AND timestamp < toDateTime('2021-04-16T23:17:00')
                    AND project_id IN tuple({self.project_id})
                    AND exception_frames.filename LIKE '%.java'
                    AND project_id IN tuple({self.project_id})
                    ORDER BY last_seen DESC
                    LIMIT 1000
                    """,
                }
            ),
        )
        assert response.status_code == 200
