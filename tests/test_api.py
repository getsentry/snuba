from __future__ import annotations

import calendar
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Generator, List, Sequence, Tuple, Union
from unittest.mock import MagicMock, patch

import pytest
import simplejson as json
from dateutil.parser import parse as parse_datetime
from sentry_sdk import Client, Hub

from snuba import settings, state
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.processor import InsertBatch, InsertEvent, ReplacementType
from snuba.redis import RedisClientKey, RedisClientType, get_redis_client
from snuba.subscriptions.store import RedisSubscriptionDataStore
from tests.base import BaseApiTest
from tests.conftest import SnubaSetConfig
from tests.helpers import write_processed_messages


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class SimpleAPITest(BaseApiTest):
    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, redis_db: None
    ) -> Generator[None, None, None]:
        # values for test data
        self.project_ids = [1, 2, 3]  # 3 projects
        self.environments = ["prød", "test"]  # 2 environments
        self.platforms = ["a", "b", "c", "d", "e", "f"]  # 6 platforms
        self.hashes = [x * 32 for x in "123456789abc"]  # 12 hashes
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=self.minutes)
        storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        assert storage is not None
        self.storage = storage
        self.table = self.storage.get_table_writer().get_schema().get_table_name()
        self.generate_fizzbuzz_events()

        yield

        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def write_events(self, events: Sequence[InsertEvent]) -> None:
        processor = self.storage.get_table_writer().get_stream_loader().get_processor()

        processed_messages = []
        for i, event in enumerate(events):
            processed_message = processor.process_message(
                (2, "insert", event, {}), KafkaMessageMetadata(i, 0, datetime.now())
            )
            assert processed_message is not None
            processed_messages.append(processed_message)

        write_processed_messages(self.storage, processed_messages)

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
                    events.append(
                        InsertEvent(
                            {
                                "organization_id": 1,
                                "project_id": p,
                                "event_id": uuid.uuid4().hex,
                                "datetime": (
                                    self.base_time + timedelta(minutes=tick)
                                ).strftime(settings.PAYLOAD_DATETIME_FORMAT),
                                "message": "a message",
                                "platform": self.platforms[
                                    (tock * p) % len(self.platforms)
                                ],
                                "primary_hash": self.hashes[
                                    (tock * p) % len(self.hashes)
                                ],
                                "group_id": self.group_ids[
                                    (tock * p) % len(self.hashes)
                                ],
                                "retention_days": settings.DEFAULT_RETENTION_DAYS,
                                "data": {
                                    # Project N sends every Nth (mod len(hashes)) hash (and platform)
                                    "received": calendar.timegm(
                                        (
                                            self.base_time + timedelta(minutes=tick)
                                        ).timetuple()
                                    ),
                                    "tags": {
                                        # Sentry
                                        "environment": self.environments[
                                            (tock * p) % len(self.environments)
                                        ],
                                        "sentry:release": str(tick),
                                        "sentry:dist": "dist1",
                                        "os.name": "windows",
                                        "os.rooted": 1,
                                        # User
                                        "foo": "baz",
                                        "foo.bar": "qux",
                                        "os_name": "linux",
                                    },
                                    "exception": {
                                        "values": [
                                            {
                                                "stacktrace": {
                                                    "frames": [
                                                        {
                                                            "filename": "foo.py",
                                                            "lineno": tock,
                                                        },
                                                        {
                                                            "filename": "bar.py",
                                                            "lineno": tock * 2,
                                                        },
                                                    ]
                                                }
                                            }
                                        ]
                                    },
                                },
                            }
                        )
                    )
        self.write_events(events)

    def redis_db_size(self, redis_client: RedisClientType) -> int:
        # dbsize could be an integer for a single node cluster or a dictionary
        # with one key value pair per node for a multi node cluster
        dbsize: int | dict[str, int] = redis_client.dbsize()
        if isinstance(dbsize, dict):
            return sum(dbsize.values())
        else:
            return dbsize


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestApi(SimpleAPITest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "events"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        self.post = _build_snql_post_methods

    def test_count(self) -> None:
        """
        Test total counts are correct in the hourly time buckets for each project
        """
        clickhouse = (
            get_storage(StorageKey.ERRORS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )
        res = clickhouse.execute("SELECT count() FROM %s" % self.table).results
        assert res[0][0] == 330

        rollup_mins = 60
        for p in self.project_ids:
            result = json.loads(
                self.post(
                    json.dumps(
                        {
                            "project": p,
                            "tenant_ids": {"referrer": "r", "organization_id": 1234},
                            "granularity": rollup_mins * 60,
                            "from_date": self.base_time.isoformat(),
                            "to_date": (
                                self.base_time + timedelta(minutes=self.minutes)
                            ).isoformat(),
                            "aggregations": [["count()", "", "aggregate"]],
                            "orderby": "time",
                            "groupby": "time",
                        }
                    ),
                ).data
            )
            buckets = self.minutes / rollup_mins
            for b in range(int(buckets)):
                bucket_time = parse_datetime(result["data"][b]["time"]).replace(
                    tzinfo=None
                )
                assert bucket_time == self.base_time + timedelta(
                    minutes=b * rollup_mins
                )
                assert result["data"][b]["aggregate"] == float(rollup_mins) / p

    def test_rollups(self) -> None:
        for rollup_mins in (1, 2, 15, 30, 60):
            # Note for buckets bigger than 1 hour, the results may not line up
            # with self.base_time as base_time is not necessarily on a bucket boundary
            result = json.loads(
                self.post(
                    json.dumps(
                        {
                            "project": 1,
                            "tenant_ids": {"referrer": "r", "organization_id": 1234},
                            "granularity": rollup_mins * 60,
                            "from_date": self.base_time.isoformat(),
                            "to_date": (
                                self.base_time + timedelta(minutes=self.minutes)
                            ).isoformat(),
                            "aggregations": [["count()", "", "aggregate"]],
                            "groupby": "time",
                            "orderby": "time",
                        }
                    ),
                ).data
            )
            buckets = self.minutes / rollup_mins
            for b in range(int(buckets)):
                bucket_time = parse_datetime(result["data"][b]["time"]).replace(
                    tzinfo=None
                )
                assert bucket_time == self.base_time + timedelta(
                    minutes=b * rollup_mins
                )
                assert (
                    result["data"][b]["aggregate"] == rollup_mins
                )  # project 1 has 1 event per minute

    def test_time_alignment(self) -> None:
        # Adding a half hour skew to the time.
        skew = timedelta(minutes=30)
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 60,
                        "selected_columns": ["time"],
                        "groupby": "time",
                        "from_date": (self.base_time + skew)
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                        "to_date": (
                            self.base_time + skew + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "time",
                    }
                ),
            ).data
        )
        bucket_time = parse_datetime(result["data"][0]["time"]).replace(tzinfo=None)
        assert bucket_time == (self.base_time + skew)

        # But if we set time alignment to an hour, the buckets will fall back to
        # the 1hr boundary.
        state.set_config("date_align_seconds", 3600)
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 60,
                        "selected_columns": ["time"],
                        "groupby": "time",
                        "from_date": (self.base_time + skew).isoformat(),
                        "to_date": (
                            self.base_time + skew + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "time",
                    }
                ),
            ).data
        )
        bucket_time = parse_datetime(result["data"][0]["time"]).replace(tzinfo=None)
        assert bucket_time == self.base_time

    def test_no_issues(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "error" not in result

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "conditions": [["group_id", "=", 100]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "error" not in result
        assert result["data"] == []

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "conditions": [["group_id", "IN", [100, 200]]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "error" not in result
        assert result["data"] == []

    def test_offset_limit(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": ["project_id"],
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "offset": 1,
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["project_id"] == 2

    def test_totals(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": ["project_id"],
                        "totals": True,
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["totals"]
        # project row
        assert result["data"][0]["project_id"] == 1
        assert result["data"][0]["count"] == 180

        # totals row
        assert (
            result["totals"]["project_id"] == 0
        )  # totals row is zero or empty for non-aggregate cols
        assert result["totals"]["count"] == 180 + 90 + 60

        # LIMIT BY
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "groupby": "environment",
                        "limitby": [100, "environment"],
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "LIMIT 100 BY _snuba_environment" in result["sql"]

        # Stress nullable totals column, making sure we get results as expected
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": [
                            "project_id",
                            "received",
                        ],  # Having received here will cause a Nullable(DateTime) column in TOTALS with the value null/None - triggering the situation we want to make sure works.
                        "totals": True,
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "WITH TOTALS" in result["sql"]

        assert len(result["data"]) == 1
        assert result["totals"]

        # totals row
        assert (
            result["totals"]["project_id"] == 0
        )  # totals row is zero or empty for non-aggregate cols
        assert result["totals"]["count"] == 180 + 90 + 60

    def test_conditions(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [
                            ["platform", "NOT IN", ["b", "c", "d", "e", "f"]]
                        ],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["platform"] == "a"

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [["message", "LIKE", "a mess%"]],
                        "orderby": "event_id",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [["message", "NOT LIKE", "a mess%"]],
                        "orderby": "event_id",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [["tags[environment]", "LIKE", "%es%"]],
                        "orderby": "event_id",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id", "received"],
                        "conditions": [["received", "=", str(self.base_time)]],
                        "orderby": "event_id",
                        "limit": 1,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "events",
                        "project": [1, 2, 3],
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": [["isNull", ["group_id"], "null_group_id"]],
                        "groupby": ["group_id"],
                        "debug": True,
                        "conditions": [
                            ["group_id", "IS NULL", None],
                            ["type", "!=", "transaction"],
                        ],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

    def test_null_array_conditions(self) -> None:
        events = []
        for value in (None, False, True):
            events.append(
                InsertEvent(
                    {
                        "organization_id": 1,
                        "project_id": 4,
                        "event_id": uuid.uuid4().hex,
                        "group_id": 1,
                        "datetime": (self.base_time).strftime(
                            settings.PAYLOAD_DATETIME_FORMAT
                        ),
                        "message": f"handled {value}",
                        "platform": "test",
                        "primary_hash": self.hashes[0],
                        "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        "data": {
                            "received": calendar.timegm(self.base_time.timetuple()),
                            "tags": {},
                            "exception": {
                                "values": [
                                    {
                                        "mechanism": {
                                            "type": "UncaughtExceptionHandler",
                                            "handled": value,
                                        },
                                        "stacktrace": {
                                            "frames": [
                                                {"filename": "foo.py", "lineno": 1},
                                            ]
                                        },
                                    }
                                ]
                            },
                        },
                    }
                )
            )
        self.write_events(events)

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 4,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["message"],
                        "conditions": [[["isHandled", []], "=", 1]],
                        "orderby": ["message"],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 2
        assert result["data"][0]["message"] == "handled None"
        assert result["data"][1]["message"] == "handled True"

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 4,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["message"],
                        "conditions": [[["notHandled", []], "=", 1]],
                        "orderby": ["message"],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["message"] == "handled False"

    def test_escaping(self) -> None:
        # Escape single quotes so we don't get Bobby Tables'd
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [
                            ["platform", "=", r"production'; DROP TABLE test; --"]
                        ],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        # Make sure we still got our table
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "aggregations": [["count()", "", "count"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"][0]["count"] == 180

        # Need to escape backslash as well otherwise the unescped
        # backslash would nullify the escaping on the quote.
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [["platform", "=", r"\'"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "error" not in result

    def test_prewhere_conditions(self) -> None:
        settings.MAX_PREWHERE_CONDITIONS = 1
        prewhere_keys = [
            "event_id",
            "group_id",
            "tags[sentry:release]",
            "message",
            "environment",
            "project_id",
        ]

        # Before we run our test, make sure the column exists in our prewhere keys
        assert "message" in prewhere_keys
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        assert (
            "PREWHERE notEquals(positionCaseInsensitive((message AS _snuba_message), 'abc'), 0)"
            in result["sql"]
        )

        # Choose the highest priority one

        # Before we run our test, make sure that we have our priorities in the test right.
        assert prewhere_keys.index("message") < prewhere_keys.index("project_id")
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert (
            "PREWHERE notEquals(positionCaseInsensitive((message AS _snuba_message)"
            in result["sql"]
        )

        # Allow 2 conditions in prewhere clause
        settings.MAX_PREWHERE_CONDITIONS = 2
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert (
            "PREWHERE notEquals(positionCaseInsensitive((message AS _snuba_message), 'abc'), 0) AND in((project_id AS _snuba_project_id), tuple(1))"
            in result["sql"]
        )

    def test_prewhere_conditions_dont_show_up_in_where_conditions(self) -> None:
        settings.MAX_PREWHERE_CONDITIONS = 1
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["event_id"],
                        "conditions": [["http_method", "=", "GET"]],
                        "limit": 1,
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        # make sure the conditions is in PREWHERE and nowhere else
        assert (
            "PREWHERE in((project_id AS _snuba_project_id), tuple(1))" in result["sql"]
        )
        assert (
            result["sql"].count("in((project_id AS _snuba_project_id), tuple(1))") == 1
        )

    def test_aggregate(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 3,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": "project_id",
                        "aggregations": [["topK(4)", "group_id", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert sorted(result["data"][0]["aggregate"]) == [
            self.group_ids[0],
            self.group_ids[3],
            self.group_ids[6],
            self.group_ids[9],
        ]

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 3,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": "project_id",
                        "aggregations": [["uniq", "group_id", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"][0]["aggregate"] == 4

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 3,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": ["project_id", "time"],
                        "aggregations": [["uniq", "group_id", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 3  # time buckets
        assert all(d["aggregate"] == 4 for d in result["data"])

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": ["project_id"],
                        "aggregations": [
                            ["count", "platform", "platforms"],
                            ["uniq", "platform", "uniq_platforms"],
                            ["topK(1)", "platform", "top_platforms"],
                        ],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        data = sorted(result["data"], key=lambda r: int(r["project_id"]))

        for idx, pid in enumerate(self.project_ids):
            assert data[idx]["project_id"] == pid
            assert data[idx]["uniq_platforms"] == len(self.platforms) // pid
            assert data[idx]["platforms"] == self.minutes // pid
            assert len(data[idx]["top_platforms"]) == 1
            assert data[idx]["top_platforms"][0] in self.platforms

    def test_aggregate_with_multiple_arguments(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 3,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": "project_id",
                        "aggregations": [
                            ["argMax", ["event_id", "timestamp"], "latest_event"]
                        ],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert "latest_event" in result["data"][0]
        assert "project_id" in result["data"][0]

    def test_having_conditions(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": "primary_hash",
                        "having": [["times_seen", ">", 1]],
                        "aggregations": [["count()", "", "times_seen"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 3

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": "primary_hash",
                        "having": [["times_seen", ">", 100]],
                        "aggregations": [["count()", "", "times_seen"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        # unknown field times_seen
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "having": [["times_seen", ">", 1]],
                        "selected_columns": ["group_id"],
                        "groupby": "time",
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert result["error"]

    def test_tag_expansion(self) -> None:
        # A promoted tag
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [["tags[sentry:dist]", "IN", ["dist1", "dist2"]]],
                        "aggregations": [["count()", "", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        # A non promoted tag
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [
                            ["tags[foo]", "=", "baz"],
                            ["tags[foo.bar]", "=", "qux"],
                        ],
                        "aggregations": [["count()", "", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        # A combination of promoted and nested
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [
                            ["tags[sentry:dist]", "=", "dist1"],
                            ["tags[foo.bar]", "=", "qux"],
                        ],
                        "aggregations": [["count()", "", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [["tags[os.rooted]", "=", "1"]],
                        "aggregations": [["count()", "", "aggregate"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

    def test_column_expansion(self) -> None:
        # If there is a condition on an already SELECTed column, then use the
        # column alias instead of the full column expression again.
        response = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "conditions": [["group_id", "=", 0], ["group_id", "=", 1]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        # Issue is expanded once, and alias used subsequently
        assert "equals(_snuba_group_id, 0)" in response["sql"]
        assert "equals(_snuba_group_id, 1)" in response["sql"]

    def test_sampling_expansion(self) -> None:
        response = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "sample": 1000,
                        "selected_columns": ["project_id"],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                )
            ).data
        )
        assert "SAMPLE 1000" in response["sql"]

        response = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 2,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "sample": 0.1,
                        "selected_columns": ["project_id"],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                )
            ).data
        )
        assert "SAMPLE 0.1" in response["sql"]

    def test_promoted_expansion(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": ["tags_key"],
                        "aggregations": [
                            ["count()", "", "count"],
                            ["uniq", "tags_value", "uniq"],
                            ["topK(3)", "tags_value", "top"],
                        ],
                        "conditions": [["tags_value", "IS NOT NULL", None]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        result_map = {d["tags_key"]: d for d in result["data"]}
        # Result contains both promoted and regular tags
        assert set(result_map.keys()) == set(
            [
                # Promoted tags
                "environment",
                "sentry:dist",
                "sentry:release",
                "os.rooted",
                "os.name",
                # User (nested) tags
                "foo",
                "foo.bar",
                # Note this is a nested (user-provided) os_name tag and is
                # unrelated to the fact that we happen to store the
                # `os.name` tag as an `os_name` column.
                "os_name",
            ]
        )

        # Reguar (nested) tag
        assert result_map["foo"]["count"] == 180
        assert len(result_map["foo"]["top"]) == 1
        assert result_map["foo"]["top"][0] == "baz"

        # Promoted tags
        assert result_map["sentry:release"]["count"] == 180
        assert len(result_map["sentry:release"]["top"]) == 3
        assert result_map["sentry:release"]["uniq"] == 180

        assert result_map["environment"]["count"] == 180
        assert len(result_map["environment"]["top"]) == 2
        assert all(r in self.environments for r in result_map["environment"]["top"])

    def test_tag_translation(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "aggregations": [["topK(100)", "tags_key", "top"]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        assert "os.rooted" in result["data"][0]["top"]

    def test_timestamp_functions(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "aggregations": [],
                    "conditions": [
                        [
                            ["coalesce", ["email", "username", "ip_address"]],
                            "=",
                            "42.200.228.8",
                        ],
                        ["project_id", "IN", [1]],
                        ["group_id", "IN", [self.group_ids[0]]],
                    ],
                    "from_date": self.base_time.isoformat(),
                    "granularity": 3600,
                    "groupby": [],
                    "having": [],
                    "limit": 51,
                    "offset": 0,
                    "orderby": ["-timestamp.to_hour"],
                    "project": [1],
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "selected_columns": [
                        [
                            "coalesce",
                            ["email", "username", "ip_address"],
                            "user.display",
                        ],
                        "release",
                        ["toStartOfHour", ["timestamp"], "timestamp.to_hour"],
                        "event_id",
                        "project_id",
                        [
                            "transform",
                            [
                                ["toString", ["project_id"]],
                                ["array", ["'1'"]],
                                ["array", ["'stuff'"]],
                                "''",
                            ],
                            "project.name",
                        ],
                    ],
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                    "totals": False,
                }
            ),
        )

        assert response.status_code == 200

    def test_tag_key_query(self) -> None:
        tags = [
            "browser",
            "environment",
            "client_os",
            "browser.name",
            "client_os.name",
        ]
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "selected_columns": [],
                        "orderby": "-count",
                        "limitby": [9, "tags_key"],
                        "project": [1],
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "dataset": "events",
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "groupby": ["tags_key", "tags_value"],
                        "conditions": [
                            ["type", "!=", "transaction"],
                            ["project_id", "IN", [1]],
                            ["tags_key", "IN", tags],
                            ["group_id", "IN", [self.group_ids[0]]],
                        ],
                        "aggregations": [
                            ["count()", "", "count"],
                            ["min", "timestamp", "first_seen"],
                            ["max", "timestamp", "last_seen"],
                        ],
                        "consistent": False,
                        "debug": False,
                    }
                ),
            ).data
        )
        formatted = sorted([f"'{t}'" for t in tags])
        tag_phrase = f"in(tupleElement(pair, 1), ({', '.join(formatted)})"
        assert tag_phrase in result["sql"]

    def test_unicode_condition(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "groupby": ["environment"],
                        "aggregations": [["count()", "", "count"]],
                        "conditions": [["environment", "IN", ["prød"]]],
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"][0] == {"environment": "prød", "count": 90}

    def test_query_too_long(self) -> None:
        long_string = "A" * 270000
        response = self.post(
            json.dumps(
                {
                    "from_date": self.base_time.isoformat(),
                    "project": [1],
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "conditions": [["platform", "NOT IN", [long_string]]],
                    "selected_columns": ["project_id"],
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert data["error"]["type"] == "query-too-long"
        assert response.status_code == 400

    def test_query_timing(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )

        assert "timing" in result
        assert "timestamp" in result["timing"]

    def test_project_rate_limiting(self) -> None:
        # All projects except project 1 are allowed
        state.set_config("project_concurrent_limit", 1)
        state.set_config("project_concurrent_limit_1", 0)

        response = self.post(
            json.dumps(
                {
                    "project": 2,
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "selected_columns": ["platform"],
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            )
        )
        assert response.status_code == 200

        response = self.post(
            json.dumps(
                {
                    "project": 1,
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "selected_columns": ["platform"],
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            )
        )
        assert response.status_code == 429
        data = json.loads(response.data)
        assert data["error"]["message"] == "project concurrent of 1 exceeds limit of 0"

    def test_doesnt_select_deletions(self) -> None:
        query = {
            "project": 1,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "groupby": "project_id",
            "aggregations": [["count()", "", "count"]],
            "from_date": self.base_time.isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        result1 = json.loads(self.post(json.dumps(query)).data)

        event_id = str(uuid.UUID("9" * 32))

        write_processed_messages(
            self.storage,
            [
                InsertBatch(
                    [
                        {
                            "event_id": event_id,
                            "project_id": 1,
                            "group_id": 1,
                            "timestamp": self.base_time,
                            "deleted": 1,
                            "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        }
                    ],
                    None,
                )
            ],
        )

        result2 = json.loads(self.post(json.dumps(query)).data)
        assert result1["data"] == result2["data"]

    def test_selected_columns(self) -> None:
        query = {
            "project": 1,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "selected_columns": ["platform", "message"],
            "orderby": "platform",
            "from_date": self.base_time.isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        result = json.loads(self.post(json.dumps(query)).data)

        assert len(result["data"]) == 180
        assert result["data"][0] == {"message": "a message", "platform": "a"}

    def test_complex_selected_columns(self) -> None:
        query = {
            "project": 1,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "selected_columns": ["platform", ["notEmpty", ["exception_stacks.type"]]],
            "from_date": self.base_time.isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        result = json.loads(self.post(json.dumps(query)).data)
        assert len(result["data"]) == 180
        assert "platform" in result["data"][0]
        assert "notEmpty(exception_stacks.type)" in result["data"][0]
        assert result["data"][0]["notEmpty(exception_stacks.type)"] == 1

        # Check that aliasing works too
        query = {
            "project": 1,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "selected_columns": [
                "platform",
                ["notEmpty", ["exception_stacks.type"], "type_not_empty"],
            ],
            "from_date": self.base_time.isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        result = json.loads(self.post(json.dumps(query)).data)
        assert len(result["data"]) == 180
        assert "platform" in result["data"][0]
        assert "type_not_empty" in result["data"][0]
        assert result["data"][0]["type_not_empty"] == 1

    def test_complex_order(self) -> None:
        # sort by a complex sort key with an expression, and a regular column,
        # and both ASC and DESC sorts.
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "selected_columns": ["environment", "time"],
                        "orderby": [["-substringUTF8", ["environment", 1, 3]], "time"],
                        "debug": True,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 180

        # all `test` events first as we sorted DESC by (the prefix of) environment
        assert all(d["environment"] == "test" for d in result["data"][:90])
        assert all(d["environment"] == "prød" for d in result["data"][90:])

        # within a value of environment, timestamps should be sorted ascending
        test_timestamps = [d["time"] for d in result["data"][:90]]
        assert sorted(test_timestamps) == test_timestamps

    def test_nullable_datetime_columns(self) -> None:
        # Test that requesting a Nullable(DateTime) column does not throw
        query = {
            "project": 1,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "selected_columns": ["received"],
            "from_date": self.base_time.isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        json.loads(self.post(json.dumps(query)).data)

    def test_duplicate_column(self) -> None:
        query = {
            "selected_columns": ["timestamp", "timestamp"],
            "limit": 3,
            "project": [1],
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "from_date": "2019-11-21T01:00:36",
            "to_date": "2019-11-26T01:00:36",
            "granularity": 3600,
        }
        result = json.loads(self.post(json.dumps(query)).data)
        assert result["meta"] == [{"name": "timestamp", "type": "DateTime"}]

    def test_exception_captured_by_sentry(self) -> None:
        events: List[Any] = []
        with Hub(Client(transport=events.append)):
            # This endpoint should return 500 as it internally raises an exception
            response = self.app.get("/tests/error")

            assert response.status_code == 500
            assert len(events) == 1
            assert events[0]["exception"]["values"][0]["type"] == "ZeroDivisionError"

    def test_split_query(self, request: pytest.FixtureRequest) -> None:
        state.set_config("use_split", 1)
        request.addfinalizer(lambda: state.set_config("use_split", 0))
        state.set_config("split_step", 3600)  # first batch will be 1 hour

        # Test getting the last 150 events, should happen in 2 batches
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 150,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, reversed(range(30, 180)))
        )

        # Test getting the last 150 events, offset by 10
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 150,
                        "offset": 10,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, reversed(range(20, 170)))
        )

        # Test asking for more events than there are
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 200,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, reversed(range(0, 180)))
        )

        # Test offset by more events than there are
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 10,
                        "offset": 180,
                    }
                ),
            ).data
        )
        assert result["data"] == []

        # Test offset that spans batches
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 10,
                        "offset": 55,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, reversed(range(115, 125)))
        )

        # Test offset by the size of the first batch retrieved. (the first batch will be discarded/trimmed)
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "limit": 10,
                        "offset": 60,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, reversed(range(110, 120)))
        )

        # Test condition that means 0 events will be returned
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": ["tags[sentry:release]", "timestamp"],
                        "conditions": [["message", "=", "doesnt exist"]],
                        "limit": 10,
                        "offset": 55,
                    }
                ),
            ).data
        )
        assert result["data"] == []

        # Test splitting query by columns - non timestamp sort
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (self.base_time + timedelta(minutes=59)).isoformat(),
                        "orderby": "tags[sentry:release]",
                        "selected_columns": [
                            "event_id",
                            "timestamp",
                            "tags[sentry:release]",
                            "tags[one]",
                            "tags[two]",
                            "tags[three]",
                            "tags[four]",
                            "tags[five]",
                        ],
                        "limit": 5,
                    }
                ),
            ).data
        )
        # Alphabetical sort
        assert [d["tags[sentry:release]"] for d in result["data"]] == [
            "0",
            "1",
            "10",
            "11",
            "12",
        ]

        # Test splitting query by columns - timestamp sort
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (self.base_time + timedelta(minutes=59)).isoformat(),
                        "orderby": "timestamp",
                        "selected_columns": [
                            "event_id",
                            "timestamp",
                            "tags[sentry:release]",
                            "tags[one]",
                            "tags[two]",
                            "tags[three]",
                            "tags[four]",
                            "tags[five]",
                        ],
                        "limit": 5,
                    }
                ),
            ).data
        )
        assert [d["tags[sentry:release]"] for d in result["data"]] == list(
            map(str, range(0, 5))
        )

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": (self.base_time - timedelta(days=100)).isoformat(),
                        "to_date": (self.base_time - timedelta(days=99)).isoformat(),
                        "orderby": "timestamp",
                        "selected_columns": [
                            "event_id",
                            "timestamp",
                            "tags[sentry:release]",
                            "tags[one]",
                            "tags[two]",
                            "tags[three]",
                            "tags[four]",
                            "tags[five]",
                        ],
                        "limit": 5,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        # Test offset
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                        "orderby": "-timestamp",
                        "selected_columns": [
                            "event_id",
                            "timestamp",
                            "tags[sentry:release]",
                            "tags[one]",
                            "tags[two]",
                            "tags[three]",
                            "tags[four]",
                            "tags[five]",
                        ],
                        "offset": 170,
                        "limit": 170,
                    }
                ),
            ).data
        )

        assert len(result["data"]) == 10
        assert [e["tags[sentry:release]"] for e in result["data"]] == list(
            map(str, reversed(range(0, 10)))
        )

    def test_consistent(self, disable_query_cache: Callable[..., Any]) -> None:
        state.set_config("consistent_override", "test_override=0;another=0.5")
        state.set_config("read_through_cache.short_circuit", 1)
        query = json.dumps(
            {
                "project": 2,
                "tenant_ids": {"referrer": "r", "organization_id": 1234},
                "aggregations": [["count()", "", "aggregate"]],
                "consistent": True,
                "debug": True,
                "from_date": self.base_time.isoformat(),
                "to_date": (
                    self.base_time + timedelta(minutes=self.minutes)
                ).isoformat(),
            }
        )

        response = json.loads(self.post(query, referrer="test_query").data)
        assert response["stats"]["consistent"]

        response = json.loads(self.post(query, referrer="test_override").data)
        assert response["stats"]["consistent"] == False

    def test_gracefully_handle_multiple_conditions_on_same_column(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "project": [2],
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "selected_columns": ["timestamp"],
                    "conditions": [
                        ["group_id", "IN", [2, 1]],
                        [["isNull", ["group_id"]], "=", 1],
                    ],
                    "debug": True,
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            ),
        )

        assert response.status_code == 200

    def test_mandatory_conditions(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "project": 1,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "granularity": 3600,
                        "selected_columns": ["group_id"],
                        "groupby": "group_id",
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            ).data
        )
        assert "deleted = 0" in result["sql"] or "equals(deleted, 0)" in result["sql"]

    def test_hierarchical_hashes_array_slice(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "project": 1,
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "granularity": 3600,
                    "selected_columns": [["arraySlice", ["hierarchical_hashes", 0, 2]]],
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            ),
        )

        assert response.status_code == 200
        result = json.loads(response.data)

        val = (
            "SELECT (arrayMap(x -> replaceAll(toString(x), '-', ''), "
            "arraySlice(hierarchical_hashes, 0, 2)) AS `_snuba_arraySlice(hierarchical_hashes, 0, 2)`)"
        )
        assert result["sql"].startswith(val)

    def test_backslashes_in_query(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "project": [1],
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "dataset": "events",
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                    "conditions": [
                        [["positionCaseInsensitive", ["message", "'Api\\'"]], "!=", 0],
                        ["environment", "IN", ["production"]],
                    ],
                    "aggregations": [["count()", "", "times_seen"]],
                    "consistent": False,
                    "debug": False,
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"times_seen": 0}]

    def test_hierarchical_hashes_array_join(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "project": 1,
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "granularity": 3600,
                    "selected_columns": [["arrayJoin", ["hierarchical_hashes"]]],
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                }
            ),
        )

        assert response.status_code == 200
        result = json.loads(response.data)

        val = (
            "SELECT (arrayJoin((arrayMap(x -> replaceAll(toString(x), '-', ''), "
            "hierarchical_hashes) AS _snuba_hierarchical_hashes)) AS `_snuba_arrayJoin(hierarchical_hashes)`)"
        )
        assert result["sql"].startswith(val)

    def test_test_endpoints(self) -> None:
        project_id = 73
        group_id = 74
        event = (
            2,
            "insert",
            {
                "event_id": "9" * 32,
                "primary_hash": "1" * 32,
                "project_id": project_id,
                "group_id": group_id,
                "datetime": self.base_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "deleted": 1,
                "retention_days": settings.DEFAULT_RETENTION_DAYS,
                "platform": "python",
                "message": "message",
                "data": {"received": time.mktime(self.base_time.timetuple())},
            },
        )
        response = self.app.post("/tests/events/eventstream", data=json.dumps(event))
        assert response.status_code == 200

        query = {
            "project": project_id,
            "tenant_ids": {"referrer": "r", "organization_id": 1234},
            "selected_columns": [],
            "groupby": "project_id",
            "aggregations": [["count()", "", "count"]],
            "conditions": [["group_id", "=", group_id]],
            "from_date": (
                self.base_time - timedelta(minutes=2 * self.minutes)
            ).isoformat(),
            "to_date": (self.base_time + timedelta(minutes=self.minutes)).isoformat(),
        }
        result = json.loads(self.post(json.dumps(query)).data)
        assert result["data"] == [{"count": 1, "project_id": project_id}]

        event = (
            2,
            ReplacementType.END_DELETE_GROUPS,
            {
                "transaction_id": "foo",
                "project_id": project_id,
                "group_ids": [group_id],
                "datetime": (self.base_time + timedelta(days=7)).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
            },
        )
        response = self.app.post("/tests/events/eventstream", data=json.dumps(event))
        assert response.status_code == 200

        result = json.loads(self.post(json.dumps(query)).data)
        assert result["data"] == []

        storage = get_writable_storage(StorageKey.ERRORS)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        )

        # There is data in the events table
        assert len(clickhouse.execute(f"SELECT * FROM {self.table}").results) > 0

        assert self.app.post("/tests/events/drop").status_code == 200
        writer = storage.get_table_writer()
        table = writer.get_schema().get_table_name()

        assert table not in clickhouse.execute("SHOW TABLES").results
        assert (
            self.redis_db_size(get_redis_client(RedisClientKey.REPLACEMENTS_STORE)) == 0
        )

        # No data in events table
        assert len(clickhouse.execute(f"SELECT * FROM {self.table}").results) == 0

    def test_max_limit(self) -> None:
        with pytest.raises(Exception):
            self.post(
                json.dumps(
                    {
                        "project": self.project_ids,
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                        "groupby": ["project_id"],
                        "selected_columns": [],
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "limit": 1000000,
                        "from_date": self.base_time.isoformat(),
                        "to_date": (
                            self.base_time + timedelta(minutes=self.minutes)
                        ).isoformat(),
                    }
                ),
            )

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    def test_record_queries(self, record_query_mock: MagicMock) -> None:
        for use_split, expected_query_count in [(0, 1), (1, 2)]:
            state.set_config("use_split", use_split)
            record_query_mock.reset_mock()
            result = json.loads(
                self.post(
                    json.dumps(
                        {
                            "project": 1,
                            "tenant_ids": {"referrer": "r", "organization_id": 1234},
                            "selected_columns": [
                                "event_id",
                                "title",
                                "transaction",
                                "tags[a]",
                                "tags[b]",
                                "message",
                                "project_id",
                            ],
                            "limit": 5,
                            "from_date": self.base_time.isoformat(),
                            "to_date": (
                                self.base_time + timedelta(minutes=self.minutes)
                            ).isoformat(),
                        }
                    ),
                ).data
            )

            assert len(result["data"]) == 5
            assert record_query_mock.call_count == 1
            metadata = record_query_mock.call_args[0][0]
            assert metadata["dataset"] == "events"
            assert metadata["request"]["referrer"] == "test"
            assert len(metadata["query_list"]) == expected_query_count

    @patch("snuba.web.query._run_query_pipeline")
    def test_error_handler(self, pipeline_mock: MagicMock) -> None:
        from redis.exceptions import ClusterDownError

        pipeline_mock.side_effect = ClusterDownError("stuff")
        response = self.post(
            json.dumps(
                {
                    "conditions": [
                        ["project_id", "IN", [1]],
                        ["group_id", "IN", [self.group_ids[0]]],
                    ],
                    "from_date": self.base_time.isoformat(),
                    "to_date": (
                        self.base_time + timedelta(minutes=self.minutes)
                    ).isoformat(),
                    "limit": 1,
                    "offset": 0,
                    "orderby": ["-timestamp", "-event_id"],
                    "project": [1],
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "selected_columns": [
                        "event_id",
                        "group_id",
                        "project_id",
                        "timestamp",
                    ],
                }
            ),
        )
        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["error"]["type"] == "internal_server_error"
        assert data["error"]["message"] == "stuff"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestCreateSubscriptionApi(BaseApiTest):
    dataset_name = "events"
    entity_key = "events"

    def test(self) -> None:
        expected_uuid = uuid.uuid1()

        with patch("snuba.subscriptions.subscription.uuid1") as uuid4:
            uuid4.return_value = expected_uuid
            resp = self.app.post(
                f"{self.dataset_name}/{self.entity_key}/subscriptions",
                data=json.dumps(
                    {
                        "project_id": 1,
                        "query": "MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
                        "time_window": int(timedelta(minutes=10).total_seconds()),
                        "resolution": int(timedelta(minutes=1).total_seconds()),
                    }
                ).encode("utf-8"),
            )

        assert resp.status_code == 202
        data = json.loads(resp.data)
        assert data == {
            "subscription_id": f"0/{expected_uuid.hex}",
        }

    def test_selected_entity_is_used(self) -> None:
        """
        Test that ensures that the passed entity is the selected one, not the dataset's default
        entity
        """

        expected_uuid = uuid.uuid1()
        entity_key = EntityKey.METRICS_COUNTERS

        with patch("snuba.subscriptions.subscription.uuid1") as uuid4:
            uuid4.return_value = expected_uuid
            resp = self.app.post(
                f"metrics/{entity_key.value}/subscriptions",
                data=json.dumps(
                    {
                        "project_id": 1,
                        "organization": 1,
                        "query": "MATCH (metrics_counters) SELECT sum(value) AS value BY "
                        "project_id, tags[3] WHERE org_id = 1 AND project_id IN array(1) AND "
                        "tags[3] IN array(1,34) AND metric_id = 7",
                        "time_window": int(timedelta(minutes=10).total_seconds()),
                        "resolution": int(timedelta(minutes=1).total_seconds()),
                    }
                ).encode("utf-8"),
            )

        assert resp.status_code == 202
        data = json.loads(resp.data)
        assert data == {
            "subscription_id": f"0/{expected_uuid.hex}",
        }
        subscription_id = data["subscription_id"]
        partition = subscription_id.split("/", 1)[0]
        assert (
            len(
                list(
                    RedisSubscriptionDataStore(
                        get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                        entity_key,
                        partition,
                    ).all()
                )
            )
            == 1
        )

    def test_invalid_dataset_and_entity_combination(self) -> None:
        expected_uuid = uuid.uuid1()
        entity_key = EntityKey.METRICS_COUNTERS
        with patch("snuba.subscriptions.subscription.uuid1") as uuid4:
            uuid4.return_value = expected_uuid
            resp = self.app.post(
                f"{self.dataset_name}/{entity_key.value}/subscriptions",
                data=json.dumps(
                    {
                        "project_id": 1,
                        "query": "MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
                        "time_window": int(timedelta(minutes=10).total_seconds()),
                        "resolution": int(timedelta(minutes=1).total_seconds()),
                        "organization": 1,
                    }
                ).encode("utf-8"),
            )

        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert data == {
            "error": {
                "message": "Invalid subscription dataset and entity combination",
                "type": "subscription",
            }
        }

    def test_time_error(self) -> None:
        resp = self.app.post(
            "{}/{}/subscriptions".format(self.dataset_name, self.entity_key),
            data=json.dumps(
                {
                    "project_id": 1,
                    "query": "MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
                    "time_window": 0,
                    "resolution": 1,
                }
            ),
        )

        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert data == {
            "error": {
                "message": "Time window must be greater than or equal to 1 minute",
                "type": "subscription",
            }
        }

    def test_with_bad_snql(self) -> None:
        expected_uuid = uuid.uuid1()

        with patch("snuba.subscriptions.subscription.uuid1") as uuid4:
            uuid4.return_value = expected_uuid
            resp = self.app.post(
                "{}/{}/subscriptions".format(self.dataset_name, self.entity_key),
                data=json.dumps(
                    {
                        "project_id": 1,
                        "time_window": int(timedelta(minutes=10).total_seconds()),
                        "resolution": int(timedelta(minutes=1).total_seconds()),
                        "query": "MATCH (events) SELECT count() AS count BY project_id WHERE platform IN tuple('a')",
                    }
                ).encode("utf-8"),
            )

        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert data == {
            "error": {
                "message": "A maximum of 1 aggregation is allowed in the select",
                "type": "invalid_query",
            }
        }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestDeleteSubscriptionApi(BaseApiTest):
    dataset_name = "events"
    dataset = get_dataset(dataset_name)

    def test(self) -> None:
        resp = self.app.post(
            f"{self.dataset_name}/events/subscriptions",
            data=json.dumps(
                {
                    "project_id": 1,
                    "query": "MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
                    "time_window": int(timedelta(minutes=10).total_seconds()),
                    "resolution": int(timedelta(minutes=1).total_seconds()),
                }
            ).encode("utf-8"),
        )

        assert resp.status_code == 202
        data = json.loads(resp.data)
        subscription_id = data["subscription_id"]
        partition = subscription_id.split("/", 1)[0]

        entity_key = EntityKey.EVENTS

        assert (
            len(
                list(
                    RedisSubscriptionDataStore(
                        get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                        entity_key,
                        partition,
                    ).all()
                )
            )
            == 1
        )

        resp = self.app.delete(
            f"{self.dataset_name}/{entity_key.value}/subscriptions/{data['subscription_id']}"
        )
        assert resp.status_code == 202, resp
        assert (
            RedisSubscriptionDataStore(
                get_redis_client(RedisClientKey.SUBSCRIPTION_STORE),
                entity_key,
                partition,
            ).all()
            == []
        )

    def test_invalid_dataset_and_entity_combination(self) -> None:
        resp = self.app.post(
            "events/metrics_counters/subscriptions",
            data=json.dumps(
                {
                    "project_id": 1,
                    "query": "MATCH (events) SELECT count() AS count WHERE platform IN tuple('a')",
                    "time_window": int(timedelta(minutes=10).total_seconds()),
                    "resolution": int(timedelta(minutes=1).total_seconds()),
                }
            ).encode("utf-8"),
        )
        assert resp.status_code == 400
        data = json.loads(resp.data)
        assert data == {
            "error": {
                "message": "Invalid subscription dataset and entity combination",
                "type": "subscription",
            }
        }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestAPIErrorsRO(TestApi):
    """
    Run the tests again, but this time on the errors_ro table to ensure they are both
    compatible.
    """

    @pytest.fixture(autouse=True)
    def use_readonly_table(self, snuba_set_config: SnubaSetConfig) -> None:
        snuba_set_config("enable_events_readonly_table", 1)
