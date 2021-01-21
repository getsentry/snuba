import pytest
import pytz
import uuid
from datetime import datetime, timedelta
from functools import partial

import simplejson as json

from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestDiscoverApi(BaseApiTest):
    @pytest.fixture(
        autouse=True, params=["/query", "/discover/snql"], ids=["legacy", "snql"]
    )
    def _set_endpoint(self, request, convert_legacy_to_snql):
        self.endpoint = request.param
        old_post = self.app.post

        if request.param == "/discover/snql":

            def new_post(endpoint, entity, data=None):
                return old_post(endpoint, data=convert_legacy_to_snql(data, entity))

        else:

            def new_post(endpoint, entity, data=None):
                return old_post(endpoint, data=data)

        self.app.post = new_post

    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=90)
        write_unprocessed_events(get_writable_storage(StorageKey.EVENTS), [self.event])
        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS), [get_raw_transaction()],
        )

    def test_raw_data(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["type", "tags[custom_tag]", "release"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "timestamp",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0] == {
            "type": "error",
            "tags[custom_tag]": "custom_value",
            "release": None,
        }

        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": [
                        "type",
                        "trace_id",
                        "tags[foo]",
                        "group_id",
                        "release",
                        "sdk_name",
                        "geo_city",
                    ],
                    "conditions": [["type", "=", "transaction"]],
                    "orderby": "timestamp",
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1, data
        assert data["data"][0] == {
            "type": "transaction",
            "trace_id": str(self.trace_id),
            "tags[foo]": "baz",
            "group_id": 0,
            "release": "1",
            "geo_city": "San Francisco",
            "sdk_name": "sentry.python",
        }

    def test_aggregations(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", None, "count"]],
                    "groupby": ["project_id", "tags[custom_tag]"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "count",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [
            {
                "count": 1,
                "tags[custom_tag]": "custom_value",
                "project_id": self.project_id,
            }
        ]

        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "groupby": ["project_id", "tags[foo]", "trace_id"],
                    "conditions": [["type", "=", "transaction"]],
                    "orderby": "count",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [
            {
                "count": 1,
                "tags[foo]": "baz",
                "project_id": self.project_id,
                "trace_id": str(self.trace_id),
            }
        ]

    def test_handles_columns_from_other_dataset(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["count()", "", "count"],
                        ["uniq", ["group_id"], "uniq_group_id"],
                        ["uniq", ["exception_stacks.type"], "uniq_ex_stacks"],
                    ],
                    "conditions": [
                        ["type", "=", "transaction"],
                        ["group_id", "=", 2],
                        ["duration", ">=", 0],
                    ],
                    "groupby": ["type"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [
            {
                "type": "transaction",
                "count": 0,
                "uniq_group_id": 0,
                "uniq_ex_stacks": None,
            }
        ]

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["uniq", ["trace_id"], "uniq_trace_id"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["type", "group_id"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert data["data"] == [
            {"type": "error", "group_id": self.event["group_id"], "uniq_trace_id": None}
        ]

    def test_geo_column_condition(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["duration", ">=", 0],
                        ["geo_country_code", "=", "MX"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 0}]

        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["duration", ">=", 0],
                        ["geo_country_code", "=", "US"],
                        ["geo_region", "=", "CA"],
                        ["geo_city", "=", "San Francisco"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_condition(self) -> None:
        if "snql" in self.endpoint:
            pytest.xfail(
                reason="snql doesn't do the implicit unpacking of array join conditions"
            )

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["exception_stacks.type", "LIKE", "Arithmetic%"],
                        ["exception_frames.filename", "LIKE", "%.java"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition(self) -> None:
        if "snql" in self.endpoint:
            pytest.xfail(
                reason="snql doesn't do the implicit unpacking of array join conditions"
            )

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count", None, "count"]],
                    "debug": True,
                    "conditions": [
                        [
                            [
                                "or",
                                [
                                    [
                                        "equals",
                                        [
                                            "exception_stacks.type",
                                            "'ArithmeticException'",
                                        ],
                                    ],
                                    [
                                        "equals",
                                        ["exception_stacks.type", "'RuntimeException'"],
                                    ],
                                ],
                            ],
                            "=",
                            1,
                        ],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition_with_arrayjoin(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count", None, "count"]],
                    "arrayjoin": "exception_stacks.type",
                    "groupby": "exception_stacks.type",
                    "debug": True,
                    "conditions": [
                        [
                            [
                                "or",
                                [
                                    [
                                        "equals",
                                        [
                                            "exception_stacks.type",
                                            "'ArithmeticException'",
                                        ],
                                    ],
                                    [
                                        "equals",
                                        ["exception_stacks.type", "'RuntimeException'"],
                                    ],
                                ],
                            ],
                            "=",
                            1,
                        ],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_exception_stack_column_boolean_condition_arrayjoin_function(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": [
                        [
                            "arrayJoin",
                            ["exception_stacks.type"],
                            "exception_stacks.type",
                        ]
                    ],
                    "aggregations": [["count", None, "count"]],
                    "groupby": "exception_stacks.type",
                    "debug": True,
                    "conditions": [
                        [
                            [
                                "or",
                                [
                                    [
                                        "equals",
                                        [
                                            "exception_stacks.type",
                                            "'ArithmeticException'",
                                        ],
                                    ],
                                    [
                                        "equals",
                                        ["exception_stacks.type", "'RuntimeException'"],
                                    ],
                                ],
                            ],
                            "=",
                            1,
                        ],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_tags_key_boolean_condition(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "turbo": False,
                    "consistent": False,
                    "aggregations": [["count", None, "count"]],
                    "conditions": [
                        [
                            [
                                "or",
                                [
                                    [
                                        "equals",
                                        [["ifNull", ["tags[foo]", "''"]], "'baz'"],
                                    ],
                                    [
                                        "equals",
                                        [["ifNull", ["tags[foo.bar]", "''"]], "'qux'"],
                                    ],
                                ],
                            ],
                            "=",
                            1,
                        ],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "groupby": "tags_key",
                    "orderby": ["-count", "tags_key"],
                    "having": [
                        [
                            "tags_key",
                            "NOT IN",
                            ["trace", "trace.ctx", "trace.span", "project"],
                        ]
                    ],
                    "project": [self.project_id],
                    "limit": 10,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200

    def test_os_fields_condition(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["duration", ">=", 0],
                        ["contexts[os.build]", "LIKE", "x86%"],
                        ["contexts[os.kernel_version]", "LIKE", "10.1%"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 0}]

    def test_http_fields(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["duration", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {
                "http_method": "POST",
                "http_referer": "tagstore.something",
                "tags[url]": "http://127.0.0.1:/query",
                "count": 1,
            }
        ]

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["group_id", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {
                "http_method": "POST",
                "http_referer": "tagstore.something",
                "tags[url]": "http://127.0.0.1:/query",
                "count": 1,
            }
        ]

    def test_device_fields_condition(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["duration", ">=", 0],
                        ["contexts[device.charging]", "=", "True"],
                        ["contexts[device.model_id]", "=", "Galaxy"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["type", "=", "error"],
                        ["contexts[device.charging]", "=", "True"],
                        ["contexts[device.model_id]", "=", "Galaxy"],
                    ],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

    def test_device_boolean_fields_context_vs_promoted_column(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["contexts[device.charging]"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["duration", ">=", 0]],
                    "groupby": ["contexts[device.charging]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["contexts[device.charging]"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["contexts[device.charging]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

    def test_is_handled(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["exception_stacks.mechanism_handled"],
                    "conditions": [
                        ["type", "=", "error"],
                        [["notHandled", []], "=", 1],
                    ],
                    "limit": 5,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["exception_stacks.mechanism_handled"] == [0]

    def test_having(self) -> None:
        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_events",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "groupby": "primary_hash",
                        "conditions": [["type", "!=", "transaction"]],
                        "having": [["times_seen", "=", 1]],
                        "aggregations": [["count()", "", "times_seen"]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

    def test_time(self) -> None:
        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_events",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["type", "!=", "transaction"]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_transactions",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["duration", ">=", 0]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

    def test_transaction_group_ids(self) -> None:
        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_transactions",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["group_id"],
                        "conditions": [
                            ["type", "=", "transaction"],
                            ["duration", ">=", 0],
                        ],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"][0]["group_id"] == 0

        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_transactions",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["group_id"],
                        "conditions": [
                            ["type", "=", "transaction"],
                            ["duration", ">=", 0],
                            ["group_id", "IN", (1, 2, 3, 4)],
                        ],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )

        assert result["data"] == []

    def test_contexts(self) -> None:
        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_events",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["type", "=", "error"]],
                        "selected_columns": ["contexts[device.online]"],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

        result = json.loads(
            self.app.post(
                self.endpoint,
                "discover_transactions",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["duration", ">=", 0]],
                        "selected_columns": ["contexts[device.online]"],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                    }
                ),
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

    def test_ast_impossible_queries(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["apdex(duration, 300)", None, "apdex_duration_300"]
                    ],
                    "groupby": ["project_id", "tags[foo]"],
                    "conditions": [],
                    "orderby": "apdex_duration_300",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [
            {
                "apdex_duration_300": 0.5,
                "tags[foo]": "baz",
                "project_id": self.project_id,
            }
        ]

    def test_count_null_user_consistency(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["uniq", "user", "uniq_user"],
                        ["count", None, "count"],
                    ],
                    "groupby": ["group_id", "user"],
                    "conditions": [],
                    "orderby": "uniq_user",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1
        assert data["data"][0]["uniq_user"] == 0

        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["uniq", "user", "uniq_user"],
                        ["count", None, "count"],
                    ],
                    "groupby": ["trace_id", "user_email"],
                    "conditions": [],
                    "orderby": "uniq_user",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1
        # Should now count '' user as Null, which is 0
        assert data["data"][0]["uniq_user"] == 0

    def test_individual_measurement(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": [
                        "event_id",
                        "measurements[lcp]",
                        "measurements[lcp.elementSize]",
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
        assert data["data"][0]["measurements[lcp]"] == 32.129
        assert data["data"][0]["measurements[lcp.elementSize]"] == 4242
        assert data["data"][0]["measurements[asd]"] is None

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id", "measurements[lcp]"],
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
        assert data["data"][0]["measurements[lcp]"] is None

    def test_functions_called_on_null(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id"],
                    "aggregations": [["sum", "duration", "sum_transaction_duration"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["group_id"],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["sum_transaction_duration"] is None

        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id"],
                    "aggregations": [
                        [
                            "quantile(0.95)",
                            "duration",
                            "quantile_0_95_transaction_duration",
                        ]
                    ],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["group_id"],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["quantile_0_95_transaction_duration"] is None

    def test_web_vitals_histogram_function(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": [
                        [
                            "arrayJoin",
                            ["measurements.key"],
                            "array_join_measurements_key",
                        ],
                        [
                            "plus",
                            [
                                [
                                    "multiply",
                                    [
                                        [
                                            "floor",
                                            [
                                                [
                                                    "divide",
                                                    [
                                                        [
                                                            "minus",
                                                            [
                                                                [
                                                                    "multiply",
                                                                    [
                                                                        [
                                                                            "arrayJoin",
                                                                            [
                                                                                "measurements.value"
                                                                            ],
                                                                        ],
                                                                        100.0,
                                                                    ],
                                                                ],
                                                                0.0,
                                                            ],
                                                        ],
                                                        1.0,
                                                    ],
                                                ]
                                            ],
                                        ],
                                        1.0,
                                    ],
                                ],
                                0.0,
                            ],
                            "measurements_histogram_1_0_100",
                        ],
                    ],
                    "aggregations": [["count", None, "count"]],
                    "conditions": [
                        ["type", "=", "transaction"],
                        ["transaction_op", "=", "pageload"],
                        ["transaction", "=", "/organizations/:orgId/issues/"],
                        ["array_join_measurements_key", "IN", ["cls"]],
                        ["measurements_histogram_1_0_100", ">=", 0],
                        ["project_id", "IN", [1]],
                    ],
                    "orderby": [
                        "measurements_histogram_1_0_100",
                        "array_join_measurements_key",
                    ],
                    "having": [],
                    "groupby": [
                        "array_join_measurements_key",
                        "measurements_histogram_1_0_100",
                    ],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data

    def test_max_timestamp_by_timestamp(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_events",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["max", "timestamp", "last_seen"]],
                    "having": [],
                    "selected_columns": ["title", "type", "timestamp"],
                    "groupby": ["title", "type", "timestamp"],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["last_seen"] is not None

    def test_invalid_event_id_condition(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "aggregations": [
                        ["divide(count(), divide(86400, 60))", None, "tpm"],
                    ],
                    "having": [],
                    "project": [self.project_id],
                    "selected_columns": ["transaction"],
                    "granularity": 3600,
                    "totals": False,
                    "conditions": [
                        ["event_id", "=", "5897895a14504192"],
                        ["type", "=", "transaction"],
                    ],
                    "groupby": ["transaction"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data

    def test_null_processor_with_if_function(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "consistent": False,
                    "having": [],
                    "aggregations": [
                        [
                            "divide",
                            [
                                [
                                    "countIf",
                                    [["greaterOrEquals", ["duration", 1000.0]]],
                                ],
                                [
                                    "if",
                                    [
                                        [
                                            "equals",
                                            [
                                                [
                                                    "countIf",
                                                    [
                                                        [
                                                            "greaterOrEquals",
                                                            ["duration", 500.0],
                                                        ]
                                                    ],
                                                ],
                                                0.0,
                                            ],
                                        ],
                                        None,
                                        [
                                            "countIf",
                                            [["greaterOrEquals", ["duration", 500.0]]],
                                        ],
                                    ],
                                ],
                            ],
                            "divide_count_at_least_transaction_duration_1000_count_at_least_transaction_duration_0",
                        ],
                    ],
                    "dataset": "discover",
                    "project": [self.project_id],
                    "selected_columns": ["transaction", "project_id"],
                    "limit": 5,
                    "offset": 0,
                    "conditions": [
                        ["type", "=", "transaction"],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "groupby": ["transaction", "project_id"],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert (
            data["data"][0][
                "divide_count_at_least_transaction_duration_1000_count_at_least_transaction_duration_0"
            ]
            == 1.0
        )

    def test_zero_literal_caching(self) -> None:
        response = self.app.post(
            self.endpoint,
            "discover_transactions",
            data=json.dumps(
                {
                    "selected_columns": [],
                    "having": [["count_unique_user", ">", 0.0]],
                    "limit": 51,
                    "offset": 0,
                    "project": [self.project_id],
                    "dataset": "discover",
                    "groupby": [],
                    "conditions": [
                        ["type", "=", "transaction"],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "aggregations": [
                        [
                            "countIf",
                            [["greaterOrEquals", ["duration", 0.0]]],
                            "count_at_least_transaction_duration_0",
                        ],
                        ["uniq", "user", "count_unique_user"],
                    ],
                    "consistent": False,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data
