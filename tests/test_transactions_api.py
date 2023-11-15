import calendar
import uuid
from collections import defaultdict
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

SNQL_ROUTE = "/transactions/snql"
LIMIT_BY_COUNT = 5


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTransactionsApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "transactions"

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
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180
        self.skew = timedelta(minutes=self.minutes)

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        ) - timedelta(minutes=self.minutes)
        self.storage = get_writable_storage(StorageKey.TRANSACTIONS)
        self.generate_fizzbuzz_events()

        yield

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
                                                "trace_id": trace_id,
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["transaction_name", "ip_address", "span_id"],
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["transaction_op", "platform", "span_id"],
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["transaction_name", "span_id"],
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id", "ip_address", "project_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "orderby": ["finish_ts"],
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id", "span_id", "project_id"],
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
        assert data["data"][0]["span_id"] == "841662216cc598b1"

    def test_trace_column_formatting(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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

    def test_individual_breakdown(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": [
                        "event_id",
                        "span_op_breakdowns[ops.db]",
                        "span_op_breakdowns[ops.http]",
                        "span_op_breakdowns[total.time]",
                        "span_op_breakdowns[not_found]",
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
        assert "span_op_breakdowns[ops.db]" in data["data"][0]
        assert "span_op_breakdowns[ops.http]" in data["data"][0]
        assert "span_op_breakdowns[total.time]" in data["data"][0]
        assert data["data"][0]["span_op_breakdowns[ops.db]"] == 62.512
        assert data["data"][0]["span_op_breakdowns[ops.http]"] == 109.774
        assert data["data"][0]["span_op_breakdowns[total.time]"] == 172.286
        assert data["data"][0]["span_op_breakdowns[not_found]"] is None

    def test_arrayjoin_measurements(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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

    def test_arrayjoin_span_op_breakdowns(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": [
                        "event_id",
                        ["arrayJoin", ["span_op_breakdowns.key"], "key"],
                        ["arrayJoin", ["span_op_breakdowns.value"], "value"],
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
        assert data["data"][0]["key"] == "ops.db"
        assert data["data"][0]["value"] == 62.512
        assert data["data"][1]["key"] == "ops.http"
        assert data["data"][1]["value"] == 109.774
        assert data["data"][2]["key"] == "total.time"
        assert data["data"][2]["value"] == 172.286
        assert data["data"][3]["key"] == "ops.db"
        assert data["data"][3]["value"] == 62.512

    def test_escaping_strings(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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

    def test_escaping_newlines(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id"],
                    "conditions": [["transaction", "LIKE", "stuff \n stuff"]],
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

    def test_escaping_not_newlines(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id"],
                    "conditions": [["transaction", "LIKE", "stuff \\n stuff"]],
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

    def test_escaping_datetimes(self) -> None:
        # Datetimes come in bizarre formats
        date_val = (self.base_time - self.skew).isoformat()
        date_val = date_val[: date_val.index("+")] + ".000000Z"
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id"],
                    "conditions": [
                        ["transaction", "=", "fake/transaction"],
                        ["finish_ts", ">=", date_val],
                        [["toStartOfHour", ["finish_ts"]], ">=", date_val],
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data

    def test_span_id(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id", "span_id"],
                    "conditions": [["span_id", "=", "841662216cc598b1"]],
                    "limit": 1,
                    "orderby": ["event_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert data["data"][0]["span_id"] == "841662216cc598b1"

        response = self.post(
            json.dumps(
                {
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": 1,
                    "selected_columns": ["event_id", "span_id"],
                    "conditions": [["span_id", "IN", ["841662216cc598b1"]]],
                    "limit": 1,
                    "orderby": ["event_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data

        assert data["data"][0]["span_id"] == "841662216cc598b1"

    def test_limitby_multicolumn(self) -> None:
        query_str = """MATCH (transactions)
                    SELECT project_id,
                           environment,
                           platform,
                           event_id
                    WHERE project_id = 1
                    AND finish_ts >= toDateTime('{start_time}')
                    AND finish_ts < toDateTime('{end_time}')
                    LIMIT {limit_by_count} BY environment, platform
                    """.format(
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
            limit_by_count=LIMIT_BY_COUNT,
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        parsed_data = json.loads(response.data)

        assert response.status_code == 200
        # Note that the following assertions will be wrong if there isn't a sufficient cross-product
        # of generated test data (meaning each environment needs to have at least
        # LIMIT_BY_COUNT records present for each platform). The number of environments
        # had to be made unequal to the number of platforms to get the test data generator
        # to write suitable input data
        assert (
            parsed_data["stats"]["result_rows"]
            == len(self.platforms) * len(self.environments) * LIMIT_BY_COUNT
        )

        records_by_limit_columns = defaultdict(list)

        for datum in parsed_data["data"]:
            records_by_limit_columns[(datum["platform"], datum["environment"])].append(
                datum
            )

        for key in records_by_limit_columns.keys():
            assert len(records_by_limit_columns[key]) == LIMIT_BY_COUNT, key

    def test_arrayjoin_multicolumn(self) -> None:
        query_str = """MATCH (transactions)
                    SELECT event_id,
                           measurements.key,
                           measurements.value
                    ARRAY JOIN measurements.key, measurements.value
                    WHERE project_id = 1
                    AND finish_ts >= toDateTime('{start_time}')
                    AND finish_ts < toDateTime('{end_time}')
                    ORDER BY event_id ASC, measurements.key ASC
                    LIMIT 4
                    """.format(
            start_time=(self.base_time - self.skew).isoformat(),
            end_time=(self.base_time + self.skew).isoformat(),
        )
        response = self.app.post(
            SNQL_ROUTE,
            data=json.dumps(
                {
                    "query": query_str,
                    "dataset": "transactions",
                    "tenant_ids": {"referrer": "r", "organization_id": 123},
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 4, data
        key_column = "measurements.key"
        value_column = "measurements.value"

        assert data["data"][0][key_column] == "lcp"
        assert data["data"][0][value_column] == 32.129
        assert data["data"][1][key_column] == "lcp.elementSize"
        assert data["data"][1][value_column] == 4242
        assert data["data"][2][key_column] == "lcp"
        assert data["data"][2][value_column] == 32.129
        assert data["data"][3][key_column] == "lcp.elementSize"
        assert data["data"][3][value_column] == 4242
