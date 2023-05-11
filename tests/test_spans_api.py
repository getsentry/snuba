from __future__ import annotations

import calendar
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Generator, Tuple, Union

import pytest
import pytz
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
            minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=self.minutes)
        self.storage = get_writable_storage(StorageKey.SPANS)
        state.set_config("spans_project_allowlist", [1])
        self.generate_fizzbuzz_events()

        yield

        state.delete_config("spans_project_allowlist")
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
                            (
                                2,
                                "insert",
                                {
                                    "project_id": p,
                                    "event_id": uuid.uuid4().hex,
                                    "deleted": 0,
                                    "datetime": (
                                        self.base_time + timedelta(minutes=tick)
                                    ).isoformat(),
                                    "platform": self.platforms[
                                        (tock * p) % len(self.platforms)
                                    ],
                                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                                    "data": {
                                        # Project N sends every Nth (mod len(hashes)) hash (and platform)
                                        "received": calendar.timegm(
                                            (
                                                self.base_time + timedelta(minutes=tick)
                                            ).timetuple()
                                        ),
                                        "type": "transaction",
                                        "transaction": "/api/do_things",
                                        "start_timestamp": datetime.timestamp(
                                            (self.base_time + timedelta(minutes=tick))
                                        ),
                                        "timestamp": datetime.timestamp(
                                            (
                                                self.base_time
                                                + timedelta(minutes=tick, seconds=1)
                                            )
                                        ),
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
                                        "user": {
                                            "email": "sally@example.org",
                                            "ip_address": "8.8.8.8",
                                        },
                                        "contexts": {
                                            "trace": {
                                                "trace_id": self.trace_id,
                                                "span_id": span_id,
                                                "op": "http",
                                                "status": "0",
                                            },
                                            "app": {"start_type": "warm"},
                                        },
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
                                        "spans": [
                                            {
                                                "op": "http.client",
                                                "trace_id": self.trace_id,
                                                "span_id": str(int(span_id, 16) + 2),
                                                "parent_span_id": span_id,
                                                "same_process_as_parent": True,
                                                "description": "GET /api/0/organizations/sentry/tags/?project=1",
                                                "data": {},
                                                "start_timestamp": calendar.timegm(
                                                    (
                                                        self.base_time
                                                        + timedelta(minutes=tick)
                                                    ).timetuple()
                                                ),
                                                "timestamp": calendar.timegm(
                                                    (
                                                        self.base_time
                                                        + timedelta(minutes=tick + 2)
                                                    ).timetuple()
                                                ),
                                                "hash": "b" * 16,
                                                "exclusive_time": 0.1234,
                                            },
                                            {
                                                "sampled": True,
                                                "same_process_as_parent": None,
                                                "description": "SELECT `sentry_tagkey`.* FROM `sentry_tagkey`",
                                                "tags": None,
                                                "start_timestamp": calendar.timegm(
                                                    (
                                                        self.base_time
                                                        + timedelta(minutes=tick + 1)
                                                    ).timetuple()
                                                ),
                                                "timestamp": calendar.timegm(
                                                    (
                                                        self.base_time
                                                        + timedelta(minutes=tick + 2)
                                                    ).timetuple()
                                                ),
                                                "parent_span_id": str(
                                                    int(span_id, 16) + 1
                                                ),
                                                "trace_id": self.trace_id,
                                                "span_id": str(int(span_id, 16) + 2),
                                                "data": {},
                                                "op": "db",
                                                "hash": "c" * 16,
                                                "exclusive_time": 0.4567,
                                            },
                                        ],
                                    },
                                },
                            ),
                            KafkaMessageMetadata(0, 0, self.base_time),
                        )
                    )
                    if processed:
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

        # No count for project 2 even though data was being sent to the processor because the
        # allowlist does not allow the project
        from_date = (self.base_time - self.skew).isoformat()
        to_date = (self.base_time + self.skew).isoformat()
        response = self._post_query(
            f"""MATCH (spans)
                SELECT count() AS aggregate
                WHERE project_id = 2
                AND timestamp >= toDateTime('{from_date}')
                AND timestamp < toDateTime('{to_date}')
            """
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["data"][0]["aggregate"] == 0, data

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
