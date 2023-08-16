from datetime import datetime, timedelta
from typing import Any, Callable, Tuple, Union

import pytest
import pytz
import simplejson as json

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.conftest import SnubaSetConfig
from tests.fixtures import get_raw_event, get_raw_transaction
from tests.helpers import write_unprocessed_events


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestDiscoverApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        # This can be overridden in the post function
        return (
            "discover_events",
            "discover",
        )

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        self.post = _build_snql_post_methods

    @pytest.fixture(scope="function", autouse=True)
    def _setup_method(self, request: Any, clickhouse_db: None) -> None:
        super().setup_method(request.function)
        self.trace_id = "7400045b25c443b885914600aa83ad04"
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=90)

        events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        assert events_storage is not None
        self.events_storage = events_storage
        write_unprocessed_events(self.events_storage, [self.event])

        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS),
            [get_raw_transaction()],
        )

    def test_raw_data(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["type", "tags[custom_tag]", "release"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "timestamp",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
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
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
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
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)

        assert response.status_code == 200

        # this generates a bad snql expression that returns a count of 0
        # rows in clickhouse 20, but on clickhouse 21 it returns no rows
        assert (
            data["data"]
            == [
                {
                    "type": "transaction",
                    "count": 0,
                    "uniq_group_id": 0,
                    "uniq_ex_stacks": None,
                }
            ]
        ) or (data["data"] == [])

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["uniq", ["transaction_status"], "uniq_transaction_status"]
                    ],
                    "conditions": [["type", "=", "error"]],
                    "groupby": ["type", "group_id"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert data["data"] == [
            {
                "type": "error",
                "group_id": self.event["group_id"],
                "uniq_transaction_status": None,
            }
        ]

    def test_geo_column_empty(self) -> None:
        event = get_raw_event()
        del event["data"]["user"]["geo"]
        write_unprocessed_events(self.events_storage, [event])

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["geo_city"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        [["isNull", ["geo_country_code"]], "!=", 1],
                        ["type", "!=", "transaction"],
                    ],
                    "groupby": ["geo_city"],
                    "orderby": "count",
                    "limit": 2,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"geo_city": "San Francisco", "count": 1}]

        transaction = get_raw_transaction()
        del transaction["data"]["user"]["geo"]
        write_unprocessed_events(
            get_writable_storage(StorageKey.TRANSACTIONS), [transaction]
        )

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["geo_city"],
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [
                        ["type", "=", "transaction"],
                        [["isNull", ["geo_country_code"]], "!=", 1],
                    ],
                    "groupby": ["geo_city"],
                    "orderby": "count",
                    "limit": 2,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"geo_city": "San Francisco", "count": 1}]

    def test_geo_column_condition(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 0}]

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition_arrayjoin_function(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_tags_key_boolean_condition(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        assert response.status_code == 200

    def test_os_fields_condition(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 0}]

    def test_http_fields(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["duration", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
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

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [["count()", "", "count"]],
                    "conditions": [["group_id", ">=", 0]],
                    "groupby": ["http_method", "http_referer", "tags[url]"],
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "parent_api": "something",
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["count"] == 1

    def test_device_boolean_fields_context_vs_promoted_column(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["contexts[device.charging]"] == "True"
        assert data["data"][0]["count"] == 1

    def test_is_handled(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"][0]["exception_stacks.mechanism_handled"] == [0]

    def test_having(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "groupby": "primary_hash",
                        "conditions": [["type", "!=", "transaction"]],
                        "having": [["times_seen", "=", 1]],
                        "aggregations": [["count()", "", "times_seen"]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

    def test_time(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["type", "!=", "transaction"]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
            ).data
        )
        assert len(result["data"]) == 1

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "selected_columns": ["project_id"],
                        "groupby": ["time", "project_id"],
                        "conditions": [["duration", ">=", 0]],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
                entity="discover_transactions",
            ).data
        )
        assert len(result["data"]) == 1

    def test_transaction_group_ids(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
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
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
                entity="discover_transactions",
            ).data
        )
        assert result["data"][0]["group_id"] == 0

        result = json.loads(
            self.post(
                json.dumps(
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
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
                entity="discover_transactions",
            ).data
        )

        assert result["data"] == []

    def test_contexts(self) -> None:
        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["type", "=", "error"]],
                        "selected_columns": ["contexts[device.online]"],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

        result = json.loads(
            self.post(
                json.dumps(
                    {
                        "dataset": "discover",
                        "project": self.project_id,
                        "conditions": [["duration", ">=", 0]],
                        "selected_columns": ["contexts[device.online]"],
                        "from_date": (self.base_time - self.skew).isoformat(),
                        "to_date": (self.base_time + self.skew).isoformat(),
                        "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    }
                ),
                entity="discover_transactions",
            ).data
        )
        assert result["data"] == [{"contexts[device.online]": "True"}]

    def test_ast_impossible_queries(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
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

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["count()", None, "count"],
                        [
                            "countIf(not(has(array(0,1,2), transaction_status)))",
                            None,
                            "failure_count",
                        ],
                    ],
                    "groupby": ["project_id", "tags[foo]"],
                    "conditions": [["server_name", "=", "TypeError"]],
                    "orderby": "count",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data["data"] == []

    def test_count_null_user_consistency(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1
        assert data["data"][0]["uniq_user"] == 0

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "aggregations": [
                        ["uniq", "user", "uniq_user"],
                        ["count", None, "count"],
                    ],
                    "groupby": ["email"],
                    "conditions": [],
                    "orderby": "uniq_user",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1
        # Should now count '' user as Null, which is 0
        assert data["data"][0]["uniq_user"] == 0

    def test_individual_measurement(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["event_id"] != "0"
        assert data["data"][0]["measurements[lcp]"] == 32.129
        assert data["data"][0]["measurements[lcp.elementSize]"] == 4242
        assert data["data"][0]["measurements[asd]"] is None

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id", "measurements[lcp]"],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert "measurements[lcp]" in data["data"][0]
        assert data["data"][0]["measurements[lcp]"] is None

    def test_individual_breakdown(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["event_id"] != "0"
        assert data["data"][0]["span_op_breakdowns[ops.db]"] == 62.512
        assert data["data"][0]["span_op_breakdowns[ops.http]"] == 109.774
        assert data["data"][0]["span_op_breakdowns[total.time]"] == 172.286
        assert data["data"][0]["span_op_breakdowns[not_found]"] is None

        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "project": self.project_id,
                    "selected_columns": ["group_id", "span_op_breakdowns[ops.db]"],
                    "limit": 1,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert "span_op_breakdowns[ops.db]" in data["data"][0]
        assert data["data"][0]["span_op_breakdowns[ops.db]"] is None

    def test_functions_called_on_null(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["sum_transaction_duration"] is None

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["quantile_0_95_transaction_duration"] is None

    def test_web_vitals_histogram_function(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data

    def test_span_op_breakdown_histogram_function(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": self.project_id,
                    "selected_columns": [
                        [
                            "arrayJoin",
                            ["span_op_breakdowns.key"],
                            "array_join_span_op_breakdowns_key",
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
                                                                                "span_op_breakdowns.value"
                                                                            ],
                                                                        ],
                                                                        1,
                                                                    ],
                                                                ],
                                                                0,
                                                            ],
                                                        ],
                                                        200,
                                                    ],
                                                ]
                                            ],
                                        ],
                                        200,
                                    ],
                                ],
                                0,
                            ],
                            "histogram_span_op_breakdowns_value_200_0_1",
                        ],
                    ],
                    "aggregations": [["count", None, "count"]],
                    "conditions": [
                        ["duration", "<", 900000],
                        ["type", "=", "transaction"],
                        ["transaction_op", "=", "pageload"],
                        [
                            "transaction",
                            "=",
                            "/organizations/:orgId/performance/summary/vitals/",
                        ],
                        [
                            "array_join_span_op_breakdowns_key",
                            "IN",
                            ["ops.http", "ops.browser", "total.time"],
                        ],
                        ["histogram_span_op_breakdowns_value_200_0_1", ">=", 0],
                        ["histogram_span_op_breakdowns_value_200_0_1", "<=", 20000],
                        ["project_id", "IN", [1]],
                    ],
                    "orderby": ["histogram_span_op_breakdowns_value_200_0_1"],
                    "having": [],
                    "groupby": [
                        "array_join_span_op_breakdowns_key",
                        "histogram_span_op_breakdowns_value_200_0_1",
                    ],
                    "limit": 300,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 0, data

    def test_max_timestamp_by_timestamp(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert len(data["data"]) == 1, data
        assert data["data"][0]["last_seen"] is not None

    def test_timestamp_functions(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "aggregations": [],
                    "conditions": [
                        ["project_id", "IN", [self.project_id]],
                        ["group_id", "IN", [2]],
                    ],
                    "granularity": 3600,
                    "groupby": [],
                    "having": [],
                    "limit": 51,
                    "offset": 0,
                    "orderby": ["-timestamp.to_day"],
                    "project": [1],
                    "selected_columns": [
                        "tags[url]",
                        ["toStartOfDay", ["timestamp"], "timestamp.to_day"],
                        "event_id",
                        "project_id",
                        [
                            "transform",
                            [
                                ["toString", ["project_id"]],
                                ["array", [f"'{self.project_id}'"]],
                                ["array", ["'things'"]],
                                "''",
                            ],
                            "project.name",
                        ],
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "totals": False,
                }
            ),
            entity="discover_events",
        )
        assert response.status_code == 200, response.data

    def test_invalid_event_id_condition(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert data["error"]["message"] == "Not a valid UUID string"

    def test_duplicate_pageload_conditions(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "selected_columns": [],
                    "having": [],
                    "limit": 51,
                    "offset": 0,
                    "project": [self.project_id],
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "groupby": [],
                    "conditions": [
                        ["duration", "<", 900000.0],
                        ["transaction_op", "=", "pageload"],
                        ["transaction_op", "=", "pageload"],
                        ["type", "=", "transaction"],
                        ["transaction", "=", "/stuff/things"],
                        [["environment", "=", "production"]],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "aggregations": [
                        ["apdex(duration, 300)", None, "apdex_300"],
                        [
                            "uniqIf",
                            ["user", ["greater", ["duration", 1200.0]]],
                            "count_miserable_user_300",
                        ],
                        ["quantile(0.95)", "duration", "p95"],
                        ["count", None, "count"],
                        ["uniq", "user", "count_unique_user"],
                        ["failure_rate()", None, "failure_rate"],
                        ["divide(count(), divide(86400, 60))", None, "tpm"],
                        [
                            "ifNull(divide(plus(uniqIf(user, greater(duration, 1200)), 5.8875), plus(uniq(user), 117.75)), 0)",
                            None,
                            "user_misery_300",
                        ],
                        [
                            "quantile(0.75)",
                            "measurements[fp]",
                            "percentile_measurements_fp_0_75",
                        ],
                        [
                            "quantile(0.75)",
                            "measurements[fcp]",
                            "percentile_measurements_fcp_0_75",
                        ],
                        [
                            "quantile(0.75)",
                            "measurements[lcp]",
                            "percentile_measurements_lcp_0_75",
                        ],
                        [
                            "quantile(0.75)",
                            "measurements[fid]",
                            "percentile_measurements_fid_0_75",
                        ],
                        [
                            "quantile(0.75)",
                            "measurements[cls]",
                            "percentile_measurements_cls_0_75",
                        ],
                    ],
                    "consistent": False,
                    "debug": False,
                }
            ),
            entity="discover_transactions",
        )
        assert response.status_code == 200

    def test_null_processor_with_if_function(self) -> None:
        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover_transactions",
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

    def test_zero_literal_caching(
        self, disable_query_cache: Callable[..., Any]
    ) -> None:
        response = self.post(
            json.dumps(
                {
                    "selected_columns": [],
                    "having": [["count_unique_user", ">", 0.0]],
                    "limit": 51,
                    "offset": 0,
                    "project": [self.project_id],
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
                    "consistent": True,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                }
            ),
            entity="discover_transactions",
        )
        data = json.loads(response.data)
        assert response.status_code == 200, response.data
        assert data["stats"]["consistent"]
        assert len(data["data"]) == 0, data

    def test_dry_run_flag(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "project": self.project_id,
                    "selected_columns": ["type", "tags[custom_tag]", "release"],
                    "conditions": [["type", "!=", "transaction"]],
                    "orderby": "timestamp",
                    "limit": 1000,
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "dry_run": True,
                }
            ),
        )
        data = json.loads(response.data)
        assert len(data["data"]) == 0

        assert data["sql"].startswith(
            "SELECT (type AS _snuba_type), (arrayElement(tags.value, indexOf(tags.key, 'custom_tag')) AS `_snuba_tags[custom_tag]`), (release AS _snuba_release)"
        )

    def test_exception_stack_column_boolean_condition_with_arrayjoin(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover_events",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {"count": 1, "exception_stacks.type": "ArithmeticException"}
        ]

    def test_exception_frames_column_boolean_condition_with_arrayjoin(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "selected_columns": [],
                    "debug": True,
                    "orderby": "-last_seen",
                    "limit": 1000,
                    "arrayjoin": "exception_frames.filename",
                    "project": [self.project_id],
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "groupby": ["exception_frames.filename"],
                    "conditions": [
                        ["exception_frames.filename", "LIKE", "%.java"],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "aggregations": [
                        ["count()", "", "times_seen"],
                        ["min", "timestamp", "first_seen"],
                        ["max", "timestamp", "last_seen"],
                    ],
                    "consistent": False,
                },
            ),
            entity="discover_events",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data["data"]) == 6

    def test_exception_stack_column_condition(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover_events",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_exception_stack_column_boolean_condition(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
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
            entity="discover_events",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [{"count": 1}]

    def test_ifnull_empty_tag_condition_replacement(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "aggregations": [],
                    "conditions": [
                        [["ifNull", ["tags[custom_tag]", "''"]], "!=", ""],
                        ["project_id", "IN", [self.project_id]],
                    ],
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "granularity": 3600,
                    "groupby": [],
                    "having": [],
                    "orderby": [],
                    "project": [self.project_id],
                    "selected_columns": [
                        "event_id",
                        "project_id",
                        [
                            "transform",
                            [
                                ["toString", ["project_id"]],
                                ["array", [f"'{self.project_id}'"]],
                                ["array", ["'sentry'"]],
                                "''",
                            ],
                            "`project.name`",
                        ],
                    ],
                    "totals": False,
                    "debug": True,
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                }
            ),
            entity="discover",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["data"] == [
            {
                "event_id": self.event["event_id"],
                "project.name": "sentry",
                "project_id": self.project_id,
            },
        ]

    def test_tagstore_sampling(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "selected_columns": [],
                    "limit": 1000,
                    "orderby": "-count",
                    "project": [self.project_id],
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "groupby": ["tags_key"],
                    "conditions": [["project_id", "IN", [self.project_id]]],
                    "aggregations": [
                        ["count()", "", "count"],
                        ["uniq", "tags_value", "values_seen"],
                    ],
                    "consistent": False,
                }
            ),
            entity="discover",
            referrer="tagstore.__get_tag_keys",
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 11

    def test_trace_sample_num_processing_errors(self) -> None:
        response = self.post(
            json.dumps(
                {
                    "selected_columns": ["trace_sampled", "num_processing_errors"],
                    "limit": 1000,
                    "project": [self.project_id],
                    "dataset": "discover",
                    "tenant_ids": {"referrer": "r", "organization_id": 1234},
                    "from_date": (self.base_time - self.skew).isoformat(),
                    "to_date": (self.base_time + self.skew).isoformat(),
                    "conditions": [["project_id", "IN", [self.project_id]]],
                    "consistent": False,
                }
            ),
            entity="discover",
        )

        assert response.status_code == 200
        data = json.loads(response.data)["data"]
        assert len(data) == 1
        assert data == [{"trace_sampled": None, "num_processing_errors": 0}]


class TestDiscoverAPIEntitySelection(TestDiscoverApi):
    """
    Override the original tests to always use the "discover" entity to test the
    entity selection.
    """

    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        # This can be overridden in the post function
        return "discover"

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        orig_post = _build_snql_post_methods

        def fixed_entity_post(
            data: str, entity: str = "discover", referrer: str = "test"
        ) -> Any:
            return orig_post(data, "discover", referrer)

        self.post = fixed_entity_post


class TestDiscoverAPIErrorsRO(TestDiscoverApi):
    """
    Override the original tests to always use the readonly table for discover_events.
    """

    @pytest.fixture(autouse=True)
    def use_readonly_table(self, snuba_set_config: SnubaSetConfig) -> None:
        snuba_set_config("enable_events_readonly_table", 1)
