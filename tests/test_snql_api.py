from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from hashlib import md5
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import simplejson as json

from snuba import state
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.query.validation.validators import ColumnValidationMode
from snuba.utils.metrics.backends.testing import get_recorded_metric_calls
from tests.base import BaseApiTest
from tests.conftest import SnubaSetConfig
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import override_entity_column_validator, write_unprocessed_events


class RejectAllocationPolicy123(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        return []

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=False,
            max_threads=0,
            explanation={"reason": "policy rejects all queries"},
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        return


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSnQLApi(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None, redis_db: None) -> None:
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.org_id = self.event["organization_id"]
        self.group_id = self.event["group_id"]
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
            get_writable_storage(StorageKey.TRANSACTIONS),
            [get_raw_transaction()],
        )

    def test_buggy_query(self) -> None:
        query = """MATCH (generic_metrics_gauges) SELECT avg(value) AS
        `aggregate_value` BY toStartOfInterval(timestamp, toIntervalSecond(1800), 'Universal') AS `time`
        WHERE granularity = 60 AND metric_id = 87269488 AND (org_id IN array(1) AND project_id IN array(1)
        AND use_case_id = 'custom') AND timestamp >= toDateTime('2023-11-27T14:00:00') AND timestamp <
        toDateTime('2023-11-28T14:30:00') ORDER BY time ASC"""
        response = self.post(
            "/generic_metrics/snql",
            data=json.dumps(
                {
                    "query": query,
                    "referrer": "myreferrer",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200, data

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
                    "referrer": "myreferrer",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                            "tenant_ids": {"referrer": "test", "organization_id": 123},
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

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    @patch("snuba.web.db_query.execute_query_with_readthrough_caching")
    def test_record_queries_on_error(
        self, execute_query_mock: MagicMock, record_query_mock: MagicMock
    ) -> None:
        from snuba.clickhouse.errors import ClickhouseError

        record_query_mock.reset_mock()
        execute_query_mock.reset_mock()
        execute_query_mock.side_effect = ClickhouseError("some error", code=1123)

        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                        SELECT event_id, title, transaction, tags[a], tags[b], message, project_id
                        WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                        AND timestamp < toDateTime('{self.next_time.isoformat()}')
                        AND project_id IN tuple({self.project_id})
                        LIMIT 5""",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 500
        metadata = record_query_mock.call_args[0][0]
        assert metadata["query_list"][0]["stats"]["error_code"] == 1123

    @patch("snuba.web.query._run_query_pipeline")
    def test_error_handler(self, pipeline_mock: MagicMock) -> None:
        from redis.exceptions import ClusterDownError

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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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

    def test_alias_limitby_aggregator(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (transactions)
                    SELECT count() AS `count`
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    AND finish_ts >= toDateTime('{self.base_time.isoformat()}')
                    AND finish_ts < toDateTime('{self.next_time.isoformat()}')
                    LIMIT 1 BY count""",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200

        data = json.loads(response.data)
        assert "LIMIT 1 BY _snuba_count" in data["sql"]

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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200

    def test_transaction_group_ids_with_results(self) -> None:
        unique = "100"
        hash = md5(unique.encode("utf-8")).hexdigest()
        group_id = int(hash[:16], 16)
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (discover)
                    SELECT count() AS count BY time
                    WHERE
                        group_ids = {group_id} AND
                        timestamp >= toDateTime('{self.base_time.isoformat()}') AND
                        timestamp < toDateTime('{self.next_time.isoformat()}') AND
                        project_id IN tuple({self.project_id})
                    ORDER BY time ASC
                    LIMIT 10000
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data[0]["count"] == 1

    def test_transaction_group_ids_with_no_results(self) -> None:
        unique = "200"
        hash = md5(unique.encode("utf-8")).hexdigest()
        group_id = int(hash[:16], 16)
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (discover)
                    SELECT count() AS count BY time
                    WHERE
                        group_ids = {group_id} AND
                        timestamp >= toDateTime('{self.base_time.isoformat()}') AND
                        timestamp < toDateTime('{self.next_time.isoformat()}') AND
                        project_id IN tuple({self.project_id})
                    ORDER BY time ASC
                    LIMIT 10000
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 0

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
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data[0]["array_spans_exclusive_time"] > 0

    @pytest.mark.parametrize(
        "url, entity",
        [
            pytest.param("/transactions/snql", "transactions", id="transactions"),
            pytest.param(
                "/discover/snql", "discover_transactions", id="discover_transactions"
            ),
        ],
    )
    def test_app_start_type(self, url: str, entity: str) -> None:
        response = self.post(
            url,
            data=json.dumps(
                {
                    "query": f"""
                    MATCH ({entity})
                    SELECT count() AS count
                    WHERE
                        finish_ts >= toDateTime('{self.base_time.isoformat()}') AND
                        finish_ts < toDateTime('{self.next_time.isoformat()}') AND
                        project_id IN tuple({self.project_id}) AND
                        app_start_type = 'warm.prewarmed'
                    LIMIT 10
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data[0]["count"] == 1

    @pytest.mark.parametrize(
        "url, entity",
        [
            pytest.param("/transactions/snql", "transactions", id="transactions"),
            pytest.param(
                "/discover/snql", "discover_transactions", id="discover_transactions"
            ),
        ],
    )
    def test_profile_id(self, url: str, entity: str) -> None:
        response = self.post(
            url,
            data=json.dumps(
                {
                    "query": f"""
                    MATCH ({entity})
                    SELECT profile_id AS profile_id
                    WHERE
                        finish_ts >= toDateTime('{self.base_time.isoformat()}') AND
                        finish_ts < toDateTime('{self.next_time.isoformat()}') AND
                        project_id IN tuple({self.project_id})
                    LIMIT 10
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data[0]["profile_id"] == "046852d24483455c8c44f0c8fbf496f9"

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
                    "app_id": "default",
                    "parent_api": "some/endpoint",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200

    def test_timing_metrics_tags(self) -> None:
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
                    "app_id": "something-good",
                    "parent_api": "some/endpoint",
                    "tenant_ids": {"referrer": "test", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200
        metric_calls = get_recorded_metric_calls("timing", "api.query")
        assert metric_calls is not None
        assert len(metric_calls) == 1
        assert metric_calls[0].tags["status"] == "success"
        assert metric_calls[0].tags["referrer"] == "test"
        assert metric_calls[0].tags["parent_api"] == "some/endpoint"
        assert metric_calls[0].tags["final"] == "False"
        assert metric_calls[0].tags["dataset"] == "events"
        assert metric_calls[0].tags["app_id"] == "something-good"

    def test_missing_alias_bug(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events)
                    SELECT group_id, count(), divide(uniq(tags[url]) AS a+*, 1)
                    BY group_id
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id = {self.project_id}
                    ORDER BY count() DESC
                    LIMIT 3
                    """,
                    "dataset": "events",
                    "team": "sns",
                    "feature": "test",
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200
        result = json.loads(response.data)
        assert len(result["data"]) > 0
        assert "count()" in result["data"][0]
        assert "divide(uniq(tags[url]) AS a+*, 1)" in result["data"][0]
        assert {"name": "count()", "type": "UInt64"} in result["meta"]
        assert {
            "name": "divide(uniq(tags[url]) AS a+*, 1)",
            "type": "Float64",
        } in result["meta"]

    def test_duplicate_alias_bug(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover)
                    SELECT count() AS count, tags[url] AS url, tags[url] AS http.url
                    BY tags[url] AS http.url, tags[url] AS url
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN array({self.project_id})
                    ORDER BY count() AS count DESC
                    LIMIT 51 OFFSET 0
                    """,
                    "dataset": "discover",
                    "team": "sns",
                    "feature": "test",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        assert response.status_code == 200
        result = json.loads(response.data)
        assert len(result["data"]) > 0
        assert "url" in result["data"][0]
        assert "http.url" in result["data"][0]
        assert {"name": "url", "type": "String"} in result["meta"]
        assert {"name": "http.url", "type": "String"} in result["meta"]

    def test_invalid_column(self) -> None:
        override_entity_column_validator(EntityKey.OUTCOMES, ColumnValidationMode.ERROR)
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
                    project_id IN tuple(123)
                LIMIT 1 OFFSET 0
                """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        override_entity_column_validator(EntityKey.OUTCOMES, ColumnValidationMode.WARN)
        assert response.status_code == 400
        assert (
            json.loads(response.data)["error"]["message"]
            == "validation failed for entity outcomes: Entity outcomes: Query column 'fake_column' does not exist"
        )

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
                    """,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
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
            "validation failed for entity events: Query column 'fsdfsd' does not exist",
            id="Invalid first Select column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.fsdfsd, e.fake_col, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: query columns (fsdfsd, fake_col) do not exist",
            id="Invalid multiple Select columns",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.group_id, gm.fsdfsd, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity groupedmessage: Query column 'fsdfsd' does not exist",
            id="Invalid second Select column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.group_id, gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.fsdfsd
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity groupedmessage: Query column 'fsdfsd' does not exist",
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
            "validation failed for entity groupedmessage: Query column 'fsdfsd' does not exist",
            id="Invalid Where column",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT e.status, gm.group_id, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: Query column 'status' does not exist",
            id="Mismatched Select columns",
        ),
        pytest.param(
            f"""{MATCH}
                    SELECT uniq(toString(e.fdsdsf)), gm.status, avg(e.retention_days) AS avg BY e.group_id, gm.status
                    {WHERE}
                    {TIMESTAMPS}
                    """,
            400,
            "validation failed for entity events: Query column 'fdsdsf' does not exist",
            id="Invalid nested column",
        ),
    ]

    @pytest.fixture()
    @pytest.mark.parametrize(
        "query, response_code, error_message", invalid_columns_composite_query_tests
    )
    def test_invalid_columns_composite_query(
        self, query: str, response_code: int, error_message: str
    ) -> None:
        override_entity_column_validator(EntityKey.EVENTS, ColumnValidationMode.ERROR)
        override_entity_column_validator(
            EntityKey.GROUPEDMESSAGE, ColumnValidationMode.ERROR
        )
        response = self.post("/events/snql", data=json.dumps({"query": query}))
        override_entity_column_validator(EntityKey.EVENTS, ColumnValidationMode.WARN)
        override_entity_column_validator(
            EntityKey.GROUPEDMESSAGE, ColumnValidationMode.WARN
        )

        assert response.status_code == response_code
        assert json.loads(response.data)["error"]["message"] == error_message

    def test_wrap_log_fn_with_ifnotfinite(self) -> None:
        """
        Test that wrapping the log function in ifNotFinite clickhouse function returns a 200 response
        """
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""
                    MATCH (events SAMPLE 1.0)
                    SELECT multiply(toUInt64(max(timestamp)), 1000) AS last_seen,
                    count() AS times_seen,
                    toUInt64(plus(multiply(log(times_seen), 600), last_seen)) AS priority,
                    uniq(group_id) AS total
                    BY group_id
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    """,
                    "dataset": "events",
                    "team": "sns",
                    "feature": "test",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200

    def test_clickhouse_type_mismatch_error(self) -> None:
        """Test that snql queries that cause Clickhosue type errors have a 400 status code."""
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover_events)
                    SELECT count()
                    WHERE type != 'transaction' AND project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN array('one', 'two')
                    LIMIT 1000""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 400

    def test_clickhouse_illegal_type_error(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover_events)
                    SELECT quantile(0.95)(type)
                    WHERE type != 'transaction' AND project_id = {self.project_id}
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    LIMIT 1000""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert b"DB::Exception: Illegal type" in response.data
        assert response.status_code == 400

    def test_invalid_tag_queries(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover)
                    SELECT count() AS `count`
                    BY time, tags[error_code] AS `error_code`, tags[count] AS `count`
                    WHERE ifNull(tags[user_flow], '') = 'buy'
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN array({self.project_id})
                    AND environment = 'www.something.com'
                    AND tags[error_code] = '2300'
                    AND tags[count] = 419
                    ORDER BY time ASC LIMIT 10000
                    GRANULARITY 3600""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                }
            ),
        )

        assert response.status_code == 400
        data = response.json
        assert data["error"]["type"] == "invalid_query"
        assert (
            data["error"]["message"]
            == "validation failed for entity discover: invalid tag condition on 'tags[count]': 419 must be a string"
        )

        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover)
                    SELECT count() AS `count`
                    BY time, tags[error_code] AS `error_code`, tags[count] AS `count`
                    WHERE ifNull(tags[user_flow], '') = 'buy'
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN array({self.project_id})
                    AND environment = 'www.something.com'
                    AND tags[error_code] IN array('2300')
                    AND tags[count] IN array(419, 70, 175, 181, 58)
                    ORDER BY time ASC LIMIT 10000
                    GRANULARITY 3600""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                }
            ),
        )

        assert response.status_code == 400
        data = response.json
        assert data["error"]["type"] == "invalid_query"
        assert (
            data["error"]["message"]
            == "validation failed for entity discover: invalid tag condition on 'tags[count]': array literal 419 must be a string"
        )

    def test_datetime_condition_types(self) -> None:
        response = self.post(
            "/discover/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (discover)
                    SELECT uniq(user) AS `count_unique_user`, uniq(user) AS `count_unique_user`
                    BY time, received AS `error.received`
                    WHERE type != 'transaction'
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN array({self.project_id})
                    AND environment = 'PROD'
                    AND received IN array('2023-01-25T20:03:13+00:00', '2023-01-27T14:14:42+00:00')
                    ORDER BY time ASC
                    LIMIT 10000
                    GRANULARITY 3600""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert (
            response.status_code == 500 or response.status_code == 400
        )  # TODO: This should be a 400, and will change once we can properly categorise these errors

    def test_timeseries_processor_join_query(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (events: events) -[attributes]-> (ga: group_attributes)
                    SELECT count() AS `count` BY events.time
                    WHERE ga.group_status IN array(0)
                    AND events.timestamp >= toDateTime('2023-11-27T10:00:00')
                    AND events.timestamp < toDateTime('2023-11-27T13:00:00')
                    AND events.project_id IN array({self.project_id})
                    AND ga.project_id IN array({self.project_id})
                    ORDER BY events.time ASC
                    LIMIT 10000
                    GRANULARITY 300""",
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert (
            "(toDateTime(multiply(intDiv(toUInt32(timestamp), 300), 300), 'Universal') AS `_snuba_events.time`)"
            in data["sql"]
        )

    def test_allocation_policy_violation(self) -> None:
        with patch(
            "snuba.web.db_query._get_allocation_policies",
            return_value=[
                RejectAllocationPolicy123(
                    StorageKey("doesntmatter"), ["a", "b", "c"], {}
                )
            ],
        ):
            response = self.post(
                "/discover/snql",
                data=json.dumps(
                    {
                        "query": f"""MATCH (discover_events)
                        SELECT quantile(0.95)(type)
                        WHERE type != 'transaction' AND project_id = {self.project_id}
                        AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                        AND timestamp < toDateTime('{self.next_time.isoformat()}')
                        LIMIT 1000""",
                        "turbo": False,
                        "consistent": True,
                        "debug": True,
                    }
                ),
            )
            assert response.status_code == 429
            assert (
                response.json["error"]["message"]
                == "{'RejectAllocationPolicy123': \"Allocation policy violated, explanation: {'reason': 'policy rejects all queries'}\"}"
            )

    def test_tags_key_column(self) -> None:
        response = self.post(
            "/events/snql",
            data=json.dumps(
                {
                    "dataset": "events",
                    "query": f"""MATCH (events)
                    SELECT count() AS `count`
                    BY tags_key
                    WHERE timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    AND project_id IN tuple({self.project_id})
                    AND group_id IN tuple({self.group_id})
                    ORDER BY count DESC""",
                    "legacy": True,
                    "app_id": "legacy",
                    "tenant_ids": {
                        "organization_id": self.org_id,
                        "referrer": "tagstore.__get_tag_keys",
                    },
                    "parent_api": "/api/0/issues|groups/{issue_id}/tags/",
                }
            ),
        )

        assert response.status_code == 200


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSnQLApiErrorsRO(TestSnQLApi):
    """
    Run the tests again, but this time on the errors_ro table to ensure they are both
    compatible.
    """

    @pytest.fixture(autouse=True)
    def use_readonly_table(self, snuba_set_config: SnubaSetConfig) -> None:
        snuba_set_config("enable_events_readonly_table", 1)
