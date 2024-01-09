from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Generator, Tuple, Union

import pytest
import simplejson as json

from snuba import settings, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestSpansApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "spans"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self,
        clickhouse_db: None,
        redis_db: None,
        _build_snql_post_methods: Callable[[str], Any],
    ) -> Generator[None, None, None]:
        self.post = _build_snql_post_methods

        # values for test data
        self.project_ids = [1, 2]  # 2 projects
        self.environments = ["prød", "staging", "test"]  # 3 environments
        self.platforms = ["a", "b"]  # 2 platforms
        self.hashes = [x * 32 for x in "0123456789ab"]  # 12 hashes
        self.minutes = 180
        self.skew = timedelta(minutes=self.minutes)
        self.trace_id = "7400045b25c443b885914600aa83ad04"

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        ) - timedelta(minutes=self.minutes)
        self.storage = get_writable_storage(StorageKey.SPANS)
        state.set_config("log_bad_span_message_percentage", 1)
        self.generate_fizzbuzz_events()

        yield

        state.delete_config("log_bad_span_message_percentage")
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def generate_fizzbuzz_events(self) -> None:
        """
        Generate a deterministic set of events across a time range.
        """
        events = []
        for tick in range(self.minutes):
            tock = tick + 1
            for p in self.project_ids:
                # project N sends an event every Nth minute
                if tock % p == 0:
                    span_id = f"8841662216cc598b{tock}"[-16:]
                    processed = (
                        self.storage.get_table_writer()
                        .get_stream_loader()
                        .get_processor()
                        .process_message(
                            {
                                "project_id": p,
                                "event_id": uuid.uuid4().hex,
                                "deleted": 0,
                                "is_segment": False,
                                "duration_ms": int(
                                    1000 * timedelta(minutes=tick).total_seconds()
                                ),
                                "start_timestamp_ms": int(
                                    1000
                                    * datetime.timestamp(
                                        self.base_time + timedelta(minutes=tick)
                                    )
                                ),
                                "received": datetime.timestamp(
                                    self.base_time + timedelta(minutes=tick)
                                ),
                                "exclusive_time_ms": int(1000 * 0.1234),
                                "trace_id": self.trace_id,
                                "span_id": span_id,
                                "retention_days": settings.DEFAULT_RETENTION_DAYS,
                                "parent_span_id": span_id,
                                "description": "GET /api/0/organizations/sentry/tags/?project=1",
                                "measurements": {
                                    "lcp": {"value": 32.129},
                                    "lcp.elementSize": {"value": 4242},
                                },
                                "breakdowns": {
                                    "span_ops": {
                                        "ops.db": {"value": 62.512},
                                        "ops.http": {"value": 109.774},
                                        "total.time": {"value": 172.286},
                                    }
                                },
                                "tags": {
                                    # Sentry
                                    "environment": self.environments[
                                        (tock * p) % len(self.environments)
                                    ],
                                    "sentry:release": str(tick),
                                    "sentry:dist": "dist1",
                                    # User
                                    "foo": "baz",
                                    "foo.bar": "qux",
                                    "os_name": "linux",
                                },
                                "sentry_tags": {
                                    "system": self.platforms[
                                        (tock * p) % len(self.platforms)
                                    ],
                                    "transaction": "/api/do_things",
                                    "transaction.op": "http",
                                    "op": "http.client",
                                    "status": "unknown",
                                    "module": "sentry",
                                    "action": "POST",
                                    "domain": "sentry.io:1234",
                                    "group": self.hashes[(tock * p) % len(self.hashes)][
                                        :16
                                    ],
                                    "sometag": "somevalue",
                                },
                            },
                            KafkaMessageMetadata(0, 0, self.base_time),
                        )
                    )
                    if p == 1:
                        assert processed is not None
                        events.append(processed)
        write_processed_messages(self.storage, events)

    def _post_query(
        self,
        query: str,
        turbo: bool = False,
        consistent: bool = True,
        debug: bool = True,
    ) -> Any:
        return self.app.post(
            "/spans/snql",
            data=json.dumps(
                {
                    "query": query,
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "tests", "organization_id": 1},
                }
            ),
            headers={"referer": "test"},
        )

    def test_count_for_project(self) -> None:
        """
        Test total counts are correct
        """
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT count() AS aggregate
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["data"][0]["aggregate"] > 10, data

    def test_get_group_sorted_by_exclusive_time(self) -> None:
        """
        Gets details about spans for a given group sorted by exclusive time
        """
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT transaction_id, segment_name, description, user, domain, span_id,
                sum(exclusive_time) AS exclusive_time
                BY transaction_id, segment_name, description, user, domain, span_id
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                ORDER BY exclusive_time DESC
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 2, data

    def test_get_all_spans_belonging_to_trace_id(self) -> None:
        """
        Gets all spans belonging to a given trace id
        """
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT span_id, description, start_timestamp, end_timestamp, op
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                AND trace_id = '{self.trace_id}'
                ORDER BY start_timestamp DESC
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 2, data

    def test_granularity_processor_gets_applied(self) -> None:
        """
        Validates that the granularity processor gets applied to queries
        """
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT count() AS aggregate
                BY time
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                GRANULARITY 60
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["sql"].startswith("SELECT (toStartOfMinute(end_timestamp")

        response = self._post_query(
            f"""MATCH (spans)
                        SELECT count() AS aggregate
                        BY time
                        WHERE project_id = 1
                        AND timestamp >= toDateTime('{from_date}')
                        AND timestamp < toDateTime('{to_date}')
                        GRANULARITY 3600
                    """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["sql"].startswith("SELECT (toStartOfHour(end_timestamp")

    def test_hex_int_column_processor_gets_applied_to_group_raw(self) -> None:
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT group_raw,
                count() AS aggregate
                BY group_raw
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                GRANULARITY 60
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["sql"].startswith(
            "SELECT (lower(hex(group_raw)) AS _snuba_group_raw)"
        )

    def test_sentry_tags_column_can_be_accessed(self) -> None:
        """
        Validates that the sentry_tags column can be accessed
        """
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT span_id, sentry_tags[sometag] AS sometag
                WHERE project_id = 1
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
                AND sentry_tags[sometag] = 'somevalue'
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) >= 1, data
