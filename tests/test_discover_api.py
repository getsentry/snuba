import uuid
from functools import partial

import simplejson as json

from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


class TestDiscoverApi(BaseApiTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})
        self.trace_id = uuid.UUID("7400045b-25c4-43b8-8591-4600aa83ad04")
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        write_unprocessed_events(
            get_writable_storage(StorageKey.EVENTS), [InsertEvent(self.event)]
        )
        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS),
            [InsertEvent(get_raw_transaction())],
        )

    def test_raw_data(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["type", "tags[custom_tag]", "release"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "timestamp",
                    "limit": 1000,
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
            "/query",
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

    def test_aggregations(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", None, "count"]],
                    "groupby": ["project_id", "tags[custom_tag]"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "count",
                    "limit": 1000,
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
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "groupby": ["project_id", "tags[foo]", "trace_id"],
                    "conditions": [["type", "=", "transaction"]],
                    "orderby": "count",
                    "limit": 1000,
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

    def test_handles_columns_from_other_dataset(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [
            {"type": "transaction", "count": 0, "uniq_group_id": 0, "uniq_ex_stacks": 0}
        ]

        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["uniq", ["trace_id"], "uniq_trace_id"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["type", "group_id"],
                    "limit": 1000,
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert data["data"] == [
            {"type": "error", "group_id": self.event["group_id"], "uniq_trace_id": 0}
        ]

    def test_geo_column_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 0}]

        response = self.app.post(
            "/query",
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
                }
            ),
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition_with_arrayjoin(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_exception_stack_column_boolean_condition_arrayjoin_function(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_tags_key_boolean_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200

    def test_os_fields_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 0}]

    def test_http_fields(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["duration", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
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
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["group_id", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
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

    def test_device_fields_condition(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

        response = self.app.post(
            "/query",
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
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

    def test_device_boolean_fields_context_vs_promoted_column(self):
        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["contexts[device.charging]"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["duration", ">=", 0]],
                    "groupby": ["contexts[device.charging]"],
                    "limit": 1000,
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

        response = self.app.post(
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["contexts[device.charging]"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["contexts[device.charging]"],
                    "limit": 1000,
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

    def test_is_handled(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["exception_stacks.mechanism_handled"] == [0]

    def test_having(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "groupby": "primary_hash",
                        "conditions": [["type", "!=", "transaction"]],
                        "having": [["times_seen", "=", 1]],
                        "aggregations": [["count()", "", "times_seen"]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

    def test_time(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["type", "!=", "transaction"]],
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
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["duration", ">=", 0]],
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

    def test_transaction_group_ids(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["group_id"],
                        "conditions": [
                            ["type", "=", "transaction"],
                            ["duration", ">=", 0],
                        ],
                    }
                ),
            ).data
        )
        assert result["data"][0]["group_id"] == 0

        result = json.loads(
            self.app.post(
                "/query",
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
                    }
                ),
            ).data
        )

        assert result["data"] == []

    def test_contexts(self):
        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["type", "=", "error"]],
                        "selected_columns": ["contexts[device.online]"],
                    }
                ),
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

        result = json.loads(
            self.app.post(
                "/query",
                data=json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["duration", ">=", 0]],
                        "selected_columns": ["contexts[device.online]"],
                    }
                ),
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

    def test_ast_impossible_queries(self):
        response = self.app.post(
            "/query",
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

    def test_count_null_user_consistency(self):
        response = self.app.post(
            "/query",
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
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1
        assert data["data"][0]["uniq_user"] == 0

        response = self.app.post(
            "/query",
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
            "/query",
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
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id", "measurements[lcp]"],
                    "limit": 1,
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
            "/query",
            data=json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id"],
                    "aggregations": [["sum", "duration", "sum_transaction_duration"]],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["group_id"],
                    "limit": 1,
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["sum_transaction_duration"] is None

        response = self.app.post(
            "/query",
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
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["quantile_0_95_transaction_duration"] is None
