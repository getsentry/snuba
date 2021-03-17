import calendar
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Union

import pytest
import pytz
import simplejson as json

from snuba import settings, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestTransactionsApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "transactions"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[str], Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Any) -> None:
        super().setup_method(test_method)

        # values for test data
        self.project_ids = [1, 2]  # 2 projects
        self.environments = [u"prød", "test"]  # 2 environments
        self.platforms = ["a", "b"]  # 2 platforms
        self.hashes = [x * 32 for x in "0123456789ab"]  # 12 hashes
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180
        self.skew = timedelta(minutes=self.minutes)

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=self.minutes)
        self.storage = get_writable_storage(StorageKey.TRANSACTIONS)
        self.generate_fizzbuzz_events()

    def teardown_method(self, test_method: Any) -> None:
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
                    trace_id = "7400045b25c443b885914600aa83ad04"
                    span_id = "8841662216cc598b"
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
                                                "trace_id": trace_id,
                                                "span_id": span_id,
                                                "op": "http",
                                                "status": "0",
                                            },
                                        },
                                        "measurements": {
                                            "lcp": {"value": 32.129},
                                            "lcp.elementSize": {"value": 4242},
                                        },
                                        "spans": [
                                            {
                                                "op": "db",
                                                "trace_id": trace_id,
                                                "span_id": span_id + "1",
                                                "parent_span_id": None,
                                                "same_process_as_parent": True,
                                                "description": "SELECT * FROM users",
                                                "data": {},
                                                "timestamp": calendar.timegm(
                                                    (
                                                        self.base_time
                                                        + timedelta(minutes=tick)
                                                    ).timetuple()
                                                ),
                                            }
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

    def test_read_ip(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["transaction_name", "ip_address"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "orderby": "start_ts",
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 1, data
        assert "ip_address" in data["data"][0]

    def test_read_lowcard(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["transaction_op", "platform"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "orderby": "start_ts",
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 1, data
        assert "platform" in data["data"][0]
        assert data["data"][0]["transaction_op"] == "http"

    def test_start_ts_microsecond_truncation(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["transaction_name"],
                    "conditions": [
                        [
                            "start_ts",
                            ">",
                            (
                                self.base_time
                                - timedelta(minutes=self.minutes, microseconds=9876)
                            ).isoformat(),
                        ],
                        [
                            "start_ts",
                            "<",
                            (
                                self.base_time
                                + timedelta(minutes=self.minutes, microseconds=9876)
                            ).isoformat(),
                        ],
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "orderby": "start_ts",
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 1, data
        assert "transaction_name" in data["data"][0]

    def test_split_query(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": [
                        "event_id",
                        "project_id",
                        "transaction_name",
                        "transaction_hash",
                        "tags_key",
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) > 1, data

    def test_column_formatting(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["event_id", "ip_address", "project_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert len(data["data"]) == 180
        first_event_id = data["data"][0]["event_id"]
        assert len(first_event_id) == 32
        assert data["data"][0]["ip_address"] == "8.8.8.8"

        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["event_id", "project_id"],
                    "conditions": [["event_id", "=", first_event_id]],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1
        assert data["data"][0]["event_id"] == first_event_id

    def test_trace_column_formatting(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["trace_id", "ip_address", "project_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert len(data["data"]) == 180
        first_trace_id = data["data"][0]["trace_id"]
        assert len(first_trace_id) == 32
        assert data["data"][0]["ip_address"] == "8.8.8.8"

        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["trace_id", "project_id"],
                    "conditions": [["trace_id", "=", first_trace_id]],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 180
        assert data["data"][0]["trace_id"] == first_trace_id

    def test_apdex_function(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["transaction_name", "duration"],
                    "aggregations": [["apdex(duration, 300)", "", "apdex_score"]],
                    "orderby": "transaction_name",
                    "groupby": ["transaction_name", "duration"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert "apdex_score" in data["data"][0]
        assert data["data"][0] == {
            "transaction_name": "/api/do_things",
            "apdex_score": 0.5,
            # we select duration to make debugging easier on failure
            "duration": 1000,
        }

    def test_failure_rate_function(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["transaction_name", "duration"],
                    "aggregations": [["failure_rate()", "", "error_percentage"]],
                    "orderby": "transaction_name",
                    "groupby": ["transaction_name", "duration"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert "error_percentage" in data["data"][0]
        assert data["data"][0] == {
            "transaction_name": "/api/do_things",
            "error_percentage": 0,
            # we select duration to make debugging easier on failure
            "duration": 1000,
        }

    def test_individual_measurement(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": [
                        "event_id",
                        "measurements[lcp]",
                        "measurements[asd]",
                    ],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert "measurements[lcp]" in data["data"][0]
        assert data["data"][0]["measurements[lcp]"] == 32.129
        assert data["data"][0]["measurements[asd]"] is None

    def test_arrayjoin_measurements(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": [
                        "event_id",
                        ["arrayJoin", ["measurements.key"], "key"],
                        ["arrayJoin", ["measurements.value"], "value"],
                    ],
                    "limit": 4,
                    "orderby": ["event_id", "key"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 4, data
        assert data["data"][0]["key"] == "lcp"
        assert data["data"][0]["value"] == 32.129
        assert data["data"][1]["key"] == "lcp.elementSize"
        assert data["data"][1]["value"] == 4242
        assert data["data"][2]["key"] == "lcp"
        assert data["data"][2]["value"] == 32.129
        assert data["data"][3]["key"] == "lcp.elementSize"
        assert data["data"][3]["value"] == 4242

    def test_escaping_strings(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "project": 1,
                    "selected_columns": ["event_id"],
                    "conditions": [["transaction", "LIKE", "stuff \\\" ' \\' stuff"]],
                    "limit": 4,
                    "orderby": ["event_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data
