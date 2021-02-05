import calendar
import time
import uuid
from datetime import datetime, timedelta
from functools import partial
from unittest.mock import patch

import pytest
import pytz
import simplejson as json
from dateutil.parser import parse as parse_datetime
from sentry_sdk import Client, Hub

from snuba import settings, state
from snuba.consumers.types import KafkaMessageMetadata
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import get_dataset
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.processor import InsertBatch
from snuba.redis import redis_client
from snuba.subscriptions.store import RedisSubscriptionDataStore
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestApi(BaseApiTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})

        # values for test data
        self.project_ids = [1, 2, 3]  # 3 projects
        self.environments = ["prød", "test"]  # 2 environments
        self.platforms = ["a", "b", "c", "d", "e", "f"]  # 6 platforms
        self.hashes = [x * 32 for x in "0123456789ab"]  # 12 hashes
        self.group_ids = [int(hsh[:16], 16) for hsh in self.hashes]
        self.minutes = 180

        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=self.minutes)
        self.storage = get_writable_storage(StorageKey.EVENTS)
        self.table = self.storage.get_table_writer().get_schema().get_table_name()
        self.generate_fizzbuzz_events()

    def teardown_method(self, test_method):
        # Reset rate limits
        state.delete_config("global_concurrent_limit")
        state.delete_config("global_per_second_limit")
        state.delete_config("project_concurrent_limit")
        state.delete_config("project_concurrent_limit_1")
        state.delete_config("project_per_second_limit")
        state.delete_config("date_align_seconds")

    def write_events(self, events):
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

    def redis_db_size(self):
        # dbsize could be an integer for a single node cluster or a dictionary
        # with one key value pair per node for a multi node cluster
        dbsize = redis_client.dbsize()
        if isinstance(dbsize, dict):
            return sum(dbsize.values())
        else:
            return dbsize

    def test_invalid_queries(self):
        result = self.app.post(
            "/query",
            data=json.dumps(
                {"project": [], "aggregations": [["count()", "", "times_seen"]]}
            ),
        )
        assert result.status_code == 400
        payload = json.loads(result.data)
        assert payload["error"]["type"] == "schema"

        result = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "project": [2],
                    "aggregations": [["parenth((eses(arehard)", "", "times_seen"]],
                }
            ),
        )
        assert result.status_code == 400
        payload = json.loads(result.data)
        assert payload["error"]["type"] == "invalid_query"

    def test_count(self):
        """
        Test total counts are correct in the hourly time buckets for each project
        """
        clickhouse = (
            get_storage(StorageKey.EVENTS)
            .get_cluster()
            .get_query_connection(ClickhouseClientSettings.QUERY)
        )
        res = clickhouse.execute("SELECT count() FROM %s" % self.table)
        assert res[0][0] == 330

        rollup_mins = 60
        for p in self.project_ids:
            result = json.loads(
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": p,
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

    def test_rollups(self):
        for rollup_mins in (1, 2, 15, 30, 60):
            # Note for buckets bigger than 1 hour, the results may not line up
            # with self.base_time as base_time is not necessarily on a bucket boundary
            result = json.loads(
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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

    def test_time_alignment(self):
        # Adding a half hour skew to the time.
        skew = timedelta(minutes=30)
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 60,
                        "groupby": "time",
                        "from_date": (self.base_time + skew)
                        .replace(tzinfo=pytz.utc)
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 60,
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

    def test_no_issues(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {"project": 1, "granularity": 3600, "groupby": "group_id"}
                ),
            ).data
        )
        assert "error" not in result

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "groupby": "group_id",
                        "conditions": [["group_id", "=", 100]],
                    }
                ),
            ).data
        )
        assert "error" not in result
        assert result["data"] == []

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "groupby": "group_id",
                        "conditions": [["group_id", "IN", [100, 200]]],
                    }
                ),
            ).data
        )
        assert "error" not in result
        assert result["data"] == []

    def test_offset_limit(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": self.project_ids,
                        "groupby": ["project_id"],
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "offset": 1,
                        "limit": 1,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["project_id"] == 2

        # limit over max-limit is invalid
        result = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "project": self.project_ids,
                    "groupby": ["project_id"],
                    "aggregations": [["count()", "", "count"]],
                    "orderby": "-count",
                    "limit": 1000000,
                }
            ),
        )
        assert result.status_code == 400

    def test_totals(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": self.project_ids,
                        "groupby": ["project_id"],
                        "totals": True,
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "limit": 1,
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": self.project_ids,
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "groupby": "environment",
                        "limitby": [100, "environment"],
                        "debug": True,
                    }
                ),
            ).data
        )
        assert "LIMIT 100 BY _snuba_environment" in result["sql"]

        # Stress nullable totals column, making sure we get results as expected
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": self.project_ids,
                        "groupby": [
                            "project_id",
                            "received",
                        ],  # Having received here will cause a Nullable(DateTime) column in TOTALS with the value null/None - triggering the situation we want to make sure works.
                        "totals": True,
                        "aggregations": [["count()", "", "count"]],
                        "orderby": "-count",
                        "limit": 1,
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

    def test_conditions(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "group_id",
                        "conditions": [[], ["group_id", "IN", self.group_ids[:5]]],
                    }
                ),
            ).data
        )
        assert set([d["group_id"] for d in result["data"]]) == set([self.group_ids[4]])

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [
                            ["platform", "NOT IN", ["b", "c", "d", "e", "f"]]
                        ],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["platform"] == "a"

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [["message", "LIKE", "a mess%"]],
                        "orderby": "event_id",
                        "limit": 1,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [["message", "NOT LIKE", "a mess%"]],
                        "orderby": "event_id",
                        "limit": 1,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [["tags[environment]", "LIKE", "%es%"]],
                        "orderby": "event_id",
                        "limit": 1,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id", "received"],
                        "conditions": [["received", "=", str(self.base_time)]],
                        "orderby": "event_id",
                        "limit": 1,
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        # Test that a scalar condition on an array column expands to an all() type
        # iterator
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": [1, 2, 3],
                        "selected_columns": ["project_id"],
                        "groupby": "project_id",
                        "conditions": [
                            # only project 1 should have an event with lineno 5
                            ["exception_frames.lineno", "=", 5],
                            ["exception_frames.filename", "LIKE", "%bar%"],
                            # TODO these conditions are both separately true for the event,
                            # but they are not both true for a single exception_frame (i.e.
                            # there is no frame with filename:bar and lineno:5). We need to
                            # fix this so that we have one iterator that tests both
                            # conditions, instead of 2 iterators testing 1 condition each.
                        ],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["project_id"] == 1

        # Test that a negative scalar condition on an array column expands to an
        # all() type iterator. Looking for stack traces that do not contain foo.py
        # at all ie. all(filename != 'foo.py')
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": [1, 2, 3],
                        "selected_columns": ["project_id"],
                        "groupby": "project_id",
                        "debug": True,
                        "conditions": [["exception_frames.filename", "!=", "foo.py"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        # Test null group_id
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": [1, 2, 3],
                        "selected_columns": ["group_id"],
                        "groupby": ["group_id"],
                        "aggregations": [["count()", "", "count"]],
                        "debug": True,
                        "conditions": [["group_id", "IS NULL", None]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        res = result["data"][0]
        assert res["group_id"] is None
        assert res["count"] > 0

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "events",
                        "project": [1, 2, 3],
                        "selected_columns": [["isNull", ["group_id"], "null_group_id"]],
                        "groupby": ["group_id"],
                        "debug": True,
                        "conditions": [["group_id", "IS NULL", None]],
                    }
                ),
            ).data
        )
        assert result["data"][0]["null_group_id"] == 1

    def test_null_array_conditions(self):
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 4,
                        "selected_columns": ["message"],
                        "conditions": [[["isHandled", []], "=", 1]],
                        "orderby": ["message"],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 2
        assert result["data"][0]["message"] == "handled None"
        assert result["data"][1]["message"] == "handled True"

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 4,
                        "selected_columns": ["message"],
                        "conditions": [[["notHandled", []], "=", 1]],
                        "orderby": ["message"],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["message"] == "handled False"

    def test_escaping(self):
        # Escape single quotes so we don't get Bobby Tables'd
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [
                            ["platform", "=", r"production'; DROP TABLE test; --"]
                        ],
                    }
                ),
            ).data
        )

        # Make sure we still got our table
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {"project": 1, "aggregations": [["count()", "", "count"]]}
                ),
            ).data
        )
        assert result["data"][0]["count"] == 180

        # Need to escape backslash as well otherwise the unescped
        # backslash would nullify the escaping on the quote.
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "aggregations": [["count()", "", "count"]],
                        "groupby": "platform",
                        "conditions": [["platform", "=", r"\'"]],
                    }
                ),
            ).data
        )
        assert "error" not in result

    def test_prewhere_conditions(self):
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
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
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [
                            [["positionCaseInsensitive", ["message", "'abc'"]], "!=", 0]
                        ],
                        "limit": 1,
                        "debug": True,
                    }
                ),
            ).data
        )
        assert (
            "PREWHERE notEquals(positionCaseInsensitive((message AS _snuba_message), 'abc'), 0) AND in(project_id, tuple(1))"
            in result["sql"]
        )

    def test_prewhere_conditions_dont_show_up_in_where_conditions(self):
        settings.MAX_PREWHERE_CONDITIONS = 1
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["event_id"],
                        "conditions": [["http_method", "=", "GET"]],
                        "limit": 1,
                        "debug": True,
                    }
                ),
            ).data
        )

        # make sure the conditions is in PREWHERE and nowhere else
        assert "PREWHERE in(project_id, tuple(1))" in result["sql"]
        assert result["sql"].count("in(project_id, tuple(1))") == 1

    def test_aggregate(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 3,
                        "groupby": "project_id",
                        "aggregations": [["topK(4)", "group_id", "aggregate"]],
                    }
                ),
            ).data
        )
        assert sorted(result["data"][0]["aggregate"]) == [
            self.group_ids[3],
            self.group_ids[6],
            self.group_ids[9],
        ]

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 3,
                        "groupby": "project_id",
                        "aggregations": [["uniq", "group_id", "aggregate"]],
                    }
                ),
            ).data
        )
        assert result["data"][0]["aggregate"] == 3

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 3,
                        "groupby": ["project_id", "time"],
                        "aggregations": [["uniq", "group_id", "aggregate"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 3  # time buckets
        assert all(d["aggregate"] == 3 for d in result["data"])

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": self.project_ids,
                        "groupby": ["project_id"],
                        "aggregations": [
                            ["count", "platform", "platforms"],
                            ["uniq", "platform", "uniq_platforms"],
                            ["topK(1)", "platform", "top_platforms"],
                        ],
                    }
                ),
            ).data
        )
        data = sorted(result["data"], key=lambda r: r["project_id"])

        for idx, pid in enumerate(self.project_ids):
            assert data[idx]["project_id"] == pid
            assert data[idx]["uniq_platforms"] == len(self.platforms) // pid
            assert data[idx]["platforms"] == self.minutes // pid
            assert len(data[idx]["top_platforms"]) == 1
            assert data[idx]["top_platforms"][0] in self.platforms

    def test_aggregate_with_multiple_arguments(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 3,
                        "groupby": "project_id",
                        "aggregations": [
                            ["argMax", ["event_id", "timestamp"], "latest_event"]
                        ],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert "latest_event" in result["data"][0]
        assert "project_id" in result["data"][0]

    def test_having_conditions(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "groupby": "primary_hash",
                        "having": [["times_seen", ">", 1]],
                        "aggregations": [["count()", "", "times_seen"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 3

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "groupby": "primary_hash",
                        "having": [["times_seen", ">", 100]],
                        "aggregations": [["count()", "", "times_seen"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 0

        # unknown field times_seen
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "having": [["times_seen", ">", 1]],
                        "groupby": "time",
                    }
                ),
            ).data
        )
        assert result["error"]

    def test_tag_expansion(self):
        # A promoted tag
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [["tags[sentry:dist]", "IN", ["dist1", "dist2"]]],
                        "aggregations": [["count()", "", "aggregate"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        # A non promoted tag
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [
                            ["tags[foo]", "=", "baz"],
                            ["tags[foo.bar]", "=", "qux"],
                        ],
                        "aggregations": [["count()", "", "aggregate"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        # A combination of promoted and nested
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [
                            ["tags[sentry:dist]", "=", "dist1"],
                            ["tags[foo.bar]", "=", "qux"],
                        ],
                        "aggregations": [["count()", "", "aggregate"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "project_id",
                        "conditions": [["tags[os.rooted]", "=", "1"]],
                        "aggregations": [["count()", "", "aggregate"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1
        assert result["data"][0]["aggregate"] == 90

    def test_column_expansion(self):
        # If there is a condition on an already SELECTed column, then use the
        # column alias instead of the full column expression again.
        response = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "granularity": 3600,
                        "groupby": "group_id",
                        "conditions": [["group_id", "=", 0], ["group_id", "=", 1]],
                    }
                ),
            ).data
        )
        # Issue is expanded once, and alias used subsequently
        assert "equals(_snuba_group_id, 0)" in response["sql"]
        assert "equals(_snuba_group_id, 1)" in response["sql"]

    def test_sampling_expansion(self):
        response = json.loads(
            self.app.post(
                "/query", data=json.dumps({"project": 2, "sample": 1000})
            ).data
        )
        assert "SAMPLE 1000" in response["sql"]

        response = json.loads(
            self.app.post("/query", data=json.dumps({"project": 2, "sample": 0.1})).data
        )
        assert "SAMPLE 0.1" in response["sql"]

    def test_promoted_expansion(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "groupby": ["tags_key"],
                        "aggregations": [
                            ["count()", "", "count"],
                            ["uniq", "tags_value", "uniq"],
                            ["topK(3)", "tags_value", "top"],
                        ],
                        "conditions": [["tags_value", "IS NOT NULL", None]],
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

    def test_tag_translation(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "aggregations": [["topK(100)", "tags_key", "top"]],
                    }
                ),
            ).data
        )

        assert "os.rooted" in result["data"][0]["top"]

    def test_unicode_condition(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "granularity": 3600,
                        "groupby": ["environment"],
                        "aggregations": [["count()", "", "count"]],
                        "conditions": [["environment", "IN", ["prød"]]],
                    }
                ),
            ).data
        )
        assert result["data"][0] == {"environment": "prød", "count": 90}

    def test_query_timing(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {"project": 1, "granularity": 3600, "groupby": "group_id"}
                ),
            ).data
        )

        assert "timing" in result
        assert "timestamp" in result["timing"]

    def test_global_rate_limiting(self):
        state.set_config("global_concurrent_limit", 0)
        response = self.app.post("/query", data=json.dumps({"project": 1}))
        assert response.status_code == 429

    def test_project_rate_limiting(self):
        # All projects except project 1 are allowed
        state.set_config("project_concurrent_limit", 1)
        state.set_config("project_concurrent_limit_1", 0)

        response = self.app.post(
            "/query", data=json.dumps({"project": 2, "selected_columns": ["platform"]})
        )
        assert response.status_code == 200

        response = self.app.post(
            "/query", data=json.dumps({"project": 1, "selected_columns": ["platform"]})
        )
        assert response.status_code == 429

    def test_doesnt_select_deletions(self):
        query = {
            "project": 1,
            "groupby": "project_id",
            "aggregations": [["count()", "", "count"]],
        }
        result1 = json.loads(self.app.post("/query", data=json.dumps(query)).data)

        write_processed_messages(
            self.storage,
            [
                InsertBatch(
                    [
                        {
                            "event_id": "9" * 32,
                            "project_id": 1,
                            "group_id": 1,
                            "timestamp": self.base_time,
                            "deleted": 1,
                            "retention_days": settings.DEFAULT_RETENTION_DAYS,
                        }
                    ]
                )
            ],
        )

        result2 = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert result1["data"] == result2["data"]

    def test_selected_columns(self):
        query = {
            "project": 1,
            "selected_columns": ["platform", "message"],
            "orderby": "platform",
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)

        assert len(result["data"]) == 180
        assert result["data"][0] == {"message": "a message", "platform": "a"}

    def test_complex_selected_columns(self):
        query = {
            "project": 1,
            "selected_columns": ["platform", ["notEmpty", ["exception_stacks.type"]]],
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert len(result["data"]) == 180
        assert "platform" in result["data"][0]
        assert "notEmpty(exception_stacks.type)" in result["data"][0]
        assert result["data"][0]["notEmpty(exception_stacks.type)"] == 1

        # Check that aliasing works too
        query = {
            "project": 1,
            "selected_columns": [
                "platform",
                ["notEmpty", ["exception_stacks.type"], "type_not_empty"],
            ],
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert len(result["data"]) == 180
        assert "platform" in result["data"][0]
        assert "type_not_empty" in result["data"][0]
        assert result["data"][0]["type_not_empty"] == 1

    def test_complex_order(self):
        # sort by a complex sort key with an expression, and a regular column,
        # and both ASC and DESC sorts.
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 1,
                        "selected_columns": ["environment", "time"],
                        "orderby": [["-substringUTF8", ["environment", 1, 3]], "time"],
                        "debug": True,
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

    def test_nullable_datetime_columns(self):
        # Test that requesting a Nullable(DateTime) column does not throw
        query = {
            "project": 1,
            "selected_columns": ["received"],
        }
        json.loads(self.app.post("/query", data=json.dumps(query)).data)

    def test_duplicate_column(self):
        query = {
            "selected_columns": ["timestamp", "timestamp"],
            "limit": 3,
            "project": [1],
            "from_date": "2019-11-21T01:00:36",
            "to_date": "2019-11-26T01:00:36",
            "granularity": 3600,
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert result["meta"] == [{"name": "timestamp", "type": "DateTime"}]

    def test_test_endpoints(self):
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
            "groupby": "project_id",
            "aggregations": [["count()", "", "count"]],
            "conditions": [["group_id", "=", group_id]],
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert result["data"] == [{"count": 1, "project_id": project_id}]

        event = (
            2,
            "end_delete_groups",
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

        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)
        assert result["data"] == []

        # make sure redis has _something_ before we go about dropping all the keys in it
        assert self.redis_db_size() > 0

        storage = get_storage(StorageKey.EVENTS)
        clickhouse = storage.get_cluster().get_query_connection(
            ClickhouseClientSettings.QUERY
        )

        # There is data in the events table
        assert len(clickhouse.execute("SELECT * FROM sentry_local")) > 0

        assert self.app.post("/tests/events/drop").status_code == 200
        writer = storage.get_table_writer()
        table = writer.get_schema().get_table_name()

        assert table not in clickhouse.execute("SHOW TABLES")
        assert self.redis_db_size() == 0

        # No data in events table
        assert len(clickhouse.execute("SELECT * FROM sentry_local")) == 0

    @pytest.mark.xfail
    def test_row_stats(self):
        query = {
            "project": 1,
            "selected_columns": ["platform"],
        }
        result = json.loads(self.app.post("/query", data=json.dumps(query)).data)

        assert "rows_read" in result["stats"]
        assert "bytes_read" in result["stats"]
        assert result["stats"]["bytes_read"] > 0

    def test_static_page_renders(self):
        response = self.app.get("/config")
        assert response.status_code == 200
        assert len(response.data) > 100

    def test_exception_captured_by_sentry(self):
        events = []
        with Hub(Client(transport=events.append)):
            # This endpoint should return 500 as it internally raises an exception
            response = self.app.get("/tests/error")

            assert response.status_code == 500
            assert len(events) == 1
            assert events[0]["exception"]["values"][0]["type"] == "ZeroDivisionError"

    def test_split_query(self):
        state.set_config("use_split", 1)
        state.set_config("split_step", 3600)  # first batch will be 1 hour
        try:

            # Test getting the last 150 events, should happen in 2 batches
            result = json.loads(
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
                            "from_date": self.base_time.isoformat(),
                            "to_date": (
                                self.base_time + timedelta(minutes=59)
                            ).isoformat(),
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
                            "from_date": self.base_time.isoformat(),
                            "to_date": (
                                self.base_time + timedelta(minutes=59)
                            ).isoformat(),
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
                            "from_date": (
                                self.base_time - timedelta(days=100)
                            ).isoformat(),
                            "to_date": (
                                self.base_time - timedelta(days=99)
                            ).isoformat(),
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
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
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

        finally:
            state.set_config("use_split", 0)

    def test_consistent(self):
        response = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "project": 2,
                        "aggregations": [["count()", "", "aggregate"]],
                        "consistent": True,
                        "debug": True,
                    }
                ),
            ).data
        )
        assert response["stats"]["consistent"]

    def test_gracefully_handle_multiple_conditions_on_same_column(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "project": [2],
                    "selected_columns": ["timestamp"],
                    "conditions": [
                        ["group_id", "IN", [2, 1]],
                        [["isNull", ["group_id"]], "=", 1],
                    ],
                    "debug": True,
                }
            ),
        )

        assert response.status_code == 200

    def test_mandatory_conditions(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {"project": 1, "granularity": 3600, "groupby": "group_id"}
                ),
            ).data
        )
        assert "deleted = 0" in result["sql"] or "equals(deleted, 0)" in result["sql"]

    @patch("snuba.settings.RECORD_QUERIES", True)
    @patch("snuba.state.record_query")
    def test_record_queries(self, record_query_mock):
        for use_split, expected_query_count in [(0, 1), (1, 2)]:
            state.set_config("use_split", use_split)
            record_query_mock.reset_mock()
            result = json.loads(
                self.app.post(
                    "/query",
                    data=json.dumps(
                        {
                            "project": 1,
                            "selected_columns": [
                                "event_id",
                                "title",
                                "transaction",
                                "tags[a]",
                                "tags[b]",
                            ],
                            "limit": 5,
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


class TestCreateSubscriptionApi(BaseApiTest):
    dataset_name = "events"

    def test(self):
        expected_uuid = uuid.uuid1()

        with patch("snuba.subscriptions.subscription.uuid1") as uuid4:
            uuid4.return_value = expected_uuid
            resp = self.app.post(
                "{}/subscriptions".format(self.dataset_name),
                data=json.dumps(
                    {
                        "project_id": 1,
                        "conditions": [["platform", "IN", ["a"]]],
                        "aggregations": [["count()", "", "count"]],
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

    def test_time_error(self):
        resp = self.app.post(
            "{}/subscriptions".format(self.dataset_name),
            data=json.dumps(
                {
                    "project_id": 1,
                    "conditions": [["platform", "IN", ["a"]]],
                    "aggregations": [["count()", "", "count"]],
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


class TestDeleteSubscriptionApi(BaseApiTest):
    dataset_name = "events"
    dataset = get_dataset(dataset_name)

    def test(self):
        resp = self.app.post(
            "{}/subscriptions".format(self.dataset_name),
            data=json.dumps(
                {
                    "project_id": 1,
                    "conditions": [],
                    "aggregations": [["count()", "", "count"]],
                    "time_window": int(timedelta(minutes=10).total_seconds()),
                    "resolution": int(timedelta(minutes=1).total_seconds()),
                }
            ).encode("utf-8"),
        )

        assert resp.status_code == 202
        data = json.loads(resp.data)
        subscription_id = data["subscription_id"]
        partition = subscription_id.split("/", 1)[0]
        assert (
            len(
                RedisSubscriptionDataStore(redis_client, self.dataset, partition,).all()
            )
            == 1
        )

        resp = self.app.delete(
            f"{self.dataset_name}/subscriptions/{data['subscription_id']}"
        )
        assert resp.status_code == 202, resp
        assert (
            RedisSubscriptionDataStore(redis_client, self.dataset, partition,).all()
            == []
        )
