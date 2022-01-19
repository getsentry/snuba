import uuid
from datetime import datetime, timedelta
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pytest
import simplejson as json

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
        state.set_config("write_span_columns_rollout_percentage", 100)
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
            "/sessions/snql",
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
            "/transactions/snql",
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
            "/transactions/snql",
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
                            WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                            AND timestamp < toDateTime('{self.next_time.isoformat()}')
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
        from rediscluster.utils import ClusterDownError

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

    def test_tags_in_groupby(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT count() AS times_seen, min(timestamp) AS first_seen, max(timestamp) AS last_seen
                    BY tags[k8s-app]
                    WHERE timestamp >= toDateTime('2021-04-06T20:42:40')
                    AND timestamp < toDateTime('2021-04-20T20:42:40')
                    AND project_id IN tuple({self.project_id}) AND tags[k8s-app] != ''
                    AND type != 'transaction'
                    AND project_id IN tuple({self.project_id})
                    ORDER BY last_seen DESC
                    LIMIT 1000
                    """,
                }
            ),
        )
        assert response.status_code == 200

    def test_escape_edge_cases(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT count() AS times_seen
                    WHERE timestamp >= toDateTime('2021-04-06T20:42:40')
                    AND timestamp < toDateTime('2021-04-20T20:42:40')
                    AND project_id IN tuple({self.project_id})
                    AND environment = '\\\\\\' \\n \\\\n \\\\'
                    """,
                }
            ),
        )
        assert response.status_code == 200

    def test_ifnull_condition_experiment(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "query": f"""MATCH (discover)
                    SELECT count() AS count
                    WHERE timestamp >= toDateTime('2021-04-06T20:42:40')
                    AND timestamp < toDateTime('2021-04-20T20:42:40')
                    AND project_id IN tuple({self.project_id})
                    AND type = 'transaction'
                    AND ifNull(tags[cache_status], '') = ''
                    AND project_id = {self.project_id}
                    AND project_id IN tuple({self.project_id})
                    LIMIT 50""",
                    "debug": True,
                }
            ),
        )
        assert response.status_code == 200

    def test_alias_allowances(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT count() AS equation[0]
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    """,
                }
            ),
        )
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 1
        assert "equation[0]" in data["data"][0]

    def test_alias_columns_in_output(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT transaction AS tn, count() AS equation[0]
                    BY project_id AS pi, transaction AS tn
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    """,
                }
            ),
        )
        assert response.status_code == 200

        data = json.loads(response.data)
        assert len(data["data"]) == 1
        assert "equation[0]" in data["data"][0]
        assert "tn" in data["data"][0]
        assert "pi" in data["data"][0]
        assert len(data["meta"]) == 3
        assert data["meta"][0] == {"name": "pi", "type": "UInt64"}
        assert data["meta"][1] == {"name": "tn", "type": "LowCardinality(String)"}
        assert data["meta"][2] == {"name": "equation[0]", "type": "UInt64"}

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
                }
            ),
        )

        assert response.status_code == 200

    def test_nullable_query(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": """
                MATCH (discover)
                SELECT uniq(sdk_version) AS count_unique_sdk_version
                WHERE
                    timestamp >= toDateTime('2021-08-18T18:34:04') AND
                    timestamp < toDateTime('2021-09-01T18:34:04') AND
                    project_id IN tuple(5433960)
                LIMIT 1 OFFSET 0
                """,
                    "turbo": False,
                    "consistent": False,
                    "debug": False,
                }
            ),
        )
        assert response.status_code == 200

    def test_suspect_spans_data(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (discover_transactions)
                    SELECT arrayReduce('sumIf', spans.exclusive_time_32, arrayMap((`x`, `y`) -> if(equals(and(equals(`x`, 'db'), equals(`y`, '05029609156d8133')), 1), 1, 0), spans.op, spans.group)) AS array_spans_exclusive_time
                    WHERE
                        transaction_name = '/api/do_things' AND
                        has(spans.op, 'db') = 1 AND
                        has(spans.group, '5029609156d8133') = 1 AND
                        duration < 900000.0 AND
                        finish_ts >= toDateTime('{self.base_time.isoformat()}') AND
                        finish_ts < toDateTime('{self.next_time.isoformat()}') AND
                        project_id IN tuple({self.project_id})
                    ORDER BY array_spans_exclusive_time DESC
                    LIMIT 10
                    """
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data[0]["array_spans_exclusive_time"] > 0

    def test_attribution_tags(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT count() AS count
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    """,
                    "team": "sns",
                    "feature": "test",
                }
            ),
        )
        assert response.status_code == 200

    def test_invalid_column(self) -> None:
        response = self.post(
            "/outcomes/snql",
            data=json.dumps(
                {
                    "query": """
                MATCH (outcomes)
                SELECT fake_column
                WHERE
                    timestamp >= toDateTime('2021-08-18T18:34:04') AND
                    timestamp < toDateTime('2021-09-01T18:34:04') AND
                    org_id = 1 AND
                    project_id IN tuple(5433960)
                LIMIT 1 OFFSET 0
                """
                }
            ),
        )
        # TODO: when validation mode is ERROR this should be:
        # assert response.status_code == 400
        # assert (
        #     json.loads(response.data)["error"]["message"]
        #     == "validation failed for entity outcomes: query column(s) fake_column do not exist"
        # )

        # For now it's 500 since it's just a clickhouse error
        assert response.status_code == 500

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
                    """
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
