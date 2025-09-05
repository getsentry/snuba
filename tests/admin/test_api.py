from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Sequence, Tuple, Type
from unittest import mock

import pytest
import simplejson as json
from attr import dataclass
from flask.testing import FlaskClient
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)

from snuba import state
from snuba.admin.auth import USER_HEADER_KEY
from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.datasets.factory import get_enabled_dataset_names
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    QueryResultOrError,
    QueryType,
    QuotaAllowance,
)
from snuba.web.rpc import RPCEndpoint
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
)

BASE_TIME = datetime.utcnow().replace(
    hour=8, minute=0, second=0, microsecond=0, tzinfo=UTC
) - timedelta(hours=24)


@dataclass
class FakeClickhouseResult:
    results: Sequence[Any]


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


@pytest.fixture(scope="session")
def rpc_test_setup() -> Tuple[Type[Any], Type[RPCEndpoint[Any, TimeSeriesResponse]]]:
    class TestRPC(RPCEndpoint[TimeSeriesRequest, TimeSeriesResponse]):
        @classmethod
        def version(cls) -> str:
            return "v1"

        @classmethod
        def request_class(cls) -> type[TimeSeriesRequest]:
            return TimeSeriesRequest

        @classmethod
        def response_class(cls) -> type[TimeSeriesResponse]:
            return TimeSeriesResponse

        def _execute(self, in_msg: TimeSeriesRequest) -> TimeSeriesResponse:
            return TimeSeriesResponse()

    return TimeSeriesRequest, TestRPC


@pytest.mark.redis_db
def test_get_configs(admin_api: FlaskClient) -> None:
    response = admin_api.get("/configs")
    assert response.status_code == 200
    assert json.loads(response.data) == []

    # Add string config
    state.set_config("cfg1", "hello world")

    # Add int config
    state.set_config("cfg2", "12")

    # Add float config
    state.set_config("cfg3", "1.0")

    # Add config with description
    state.set_config("cfg4", "test")
    state.set_config_description("cfg4", "test desc")

    response = admin_api.get("/configs")
    assert response.status_code == 200
    assert json.loads(response.data) == [
        {"key": "cfg1", "type": "string", "value": "hello world", "description": None},
        {"key": "cfg2", "type": "int", "value": "12", "description": None},
        {"key": "cfg3", "type": "float", "value": "1.0", "description": None},
        {"key": "cfg4", "type": "string", "value": "test", "description": "test desc"},
    ]


@pytest.mark.redis_db
def test_post_configs(admin_api: FlaskClient) -> None:
    # int
    response = admin_api.post(
        "/configs",
        data=json.dumps({"key": "test_int", "value": "1", "description": "test int"}),
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "key": "test_int",
        "value": "1",
        "type": "int",
        "description": "test int",
    }

    # float
    response = admin_api.post(
        "/configs",
        data=json.dumps({"key": "test_float", "value": "0.1", "description": "test float"}),
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "key": "test_float",
        "value": "0.1",
        "type": "float",
        "description": "test float",
    }

    # string
    response = admin_api.post(
        "/configs",
        data=json.dumps({"key": "test_string", "value": "foo", "description": "test string"}),
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "key": "test_string",
        "value": "foo",
        "type": "string",
        "description": "test string",
    }

    # reject duplicate key
    response = admin_api.post(
        "/configs",
        data=json.dumps({"key": "test_string", "value": "bar", "description": "test string 2"}),
    )
    assert response.status_code == 400


@pytest.mark.redis_db
def test_delete_configs(admin_api: FlaskClient) -> None:
    # delete a config and its description
    state.set_config("delete_this", "1")
    state.set_config_description("delete_this", "description for this config")
    assert state.get_uncached_config("delete_this") == 1
    assert state.get_config_description("delete_this") == "description for this config"

    response = admin_api.delete("/configs/delete_this")

    assert response.status_code == 200
    assert state.get_uncached_config("delete_this") is None
    assert state.get_config_description("delete_this") is None

    # delete a config with '/' in it and its description
    state.set_config("delete/with/slash", "1")
    state.set_config_description("delete/with/slash", "description for delete with slash config")
    assert state.get_uncached_config("delete/with/slash") == 1
    assert (
        state.get_config_description("delete/with/slash")
        == "description for delete with slash config"
    )

    response = admin_api.delete("configs/delete/with/slash")

    assert response.status_code == 200
    assert state.get_uncached_config("delete/with/slash") is None
    assert state.get_config_description("delete/with/slash") is None

    # delete a config but not description
    state.set_config("delete_this", "1")
    state.set_config_description("delete_this", "description for this config")
    assert state.get_uncached_config("delete_this") == 1
    assert state.get_config_description("delete_this") == "description for this config"

    response = admin_api.delete("/configs/delete_this?keepDescription=true")

    assert response.status_code == 200
    assert state.get_uncached_config("delete_this") is None
    assert state.get_config_description("delete_this") == "description for this config"


@pytest.mark.redis_db
def test_config_descriptions(admin_api: FlaskClient) -> None:
    state.set_config_description("desc_test", "description test")
    state.set_config_description("another_test", "another description")
    response = admin_api.get("/all_config_descriptions")
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "desc_test": "description test",
        "another_test": "another description",
    }


@pytest.mark.redis_db
def get_node_for_table(admin_api: FlaskClient, storage_name: str) -> tuple[str, str, int]:
    response = admin_api.get("/clickhouse_nodes")
    assert response.status_code == 200, response
    nodes = json.loads(response.data)
    for node in nodes:
        if node["storage_name"] == storage_name:
            table = node["local_table_name"]
            host = node["local_nodes"][0]["host"]
            port = node["local_nodes"][0]["port"]
            return str(table), str(host), int(port)

    raise Exception(f"{storage_name} does not have a local node")


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_system_query(admin_api: FlaskClient) -> None:
    _, host, port = get_node_for_table(admin_api, "errors")
    response = admin_api.post(
        "/run_clickhouse_system_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "host": host,
                "port": port,
                "storage": "errors_ro",
                "sql": "SELECT count(), is_currently_executing from system.replication_queue GROUP BY is_currently_executing",
            }
        ),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["column_names"] == ["count()", "is_currently_executing"]
    assert data["rows"] == []


@pytest.mark.redis_db
def test_predefined_system_queries(admin_api: FlaskClient) -> None:
    response = admin_api.get(
        "/clickhouse_queries",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) > 1
    assert data[0]["description"] == "Currently executing merges"
    assert data[0]["name"] == "CurrentMerges"


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_sudo_system_query(admin_api: FlaskClient) -> None:
    _, host, port = get_node_for_table(admin_api, "errors")
    response = admin_api.post(
        "/run_clickhouse_system_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps(
            {
                "host": host,
                "port": port,
                "storage": "errors_ro",
                "sql": "SYSTEM START MERGES",
                "sudo": True,
            }
        ),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["column_names"] == []
    assert data["rows"] == []


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_trace(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"storage": "errors_ro", "sql": f"SELECT count() FROM {table}"}),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "<Debug> executeQuery" in data["trace_output"]
    assert "summarized_trace_output" in data
    assert "profile_events_results" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_trace_bad_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"storage": "errors_ro", "sql": f"SELECT count(asdasds) FROM {table}"}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    # error message is different in CH versions 23.8 and 24.3
    assert (
        "Exception: Unknown expression or function identifier" in data["error"]["message"]
        or "Exception: Missing columns" in data["error"]["message"]
    )
    assert "clickhouse" == data["error"]["type"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_trace_invalid_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"storage": "errors_ro", "sql": f"SELECT count() FROM {table};"}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert "; is not allowed in the query" in data["error"]["message"]
    assert "validation" == data["error"]["type"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_querylog_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "querylog")
    response = admin_api.post(
        "/clickhouse_querylog_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps({"sql": f"SELECT count() FROM {table}"}),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "column_names" in data and data["column_names"] == ["count()"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_querylog_invalid_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_querylog_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps({"sql": f"SELECT count() FROM {table}"}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert "error" in data and data["error"]["message"].startswith("Invalid FROM")


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_querylog_describe(admin_api: FlaskClient) -> None:
    response = admin_api.get("/clickhouse_querylog_schema")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "column_names" in data and "rows" in data


@pytest.mark.redis_db
def test_predefined_querylog_queries(admin_api: FlaskClient) -> None:
    response = admin_api.get(
        "/querylog_queries",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) > 1
    assert data[0]["description"] == "Find a query by its ID"
    assert data[0]["name"] == "QueryByID"


@pytest.mark.redis_db
def test_get_snuba_datasets(admin_api: FlaskClient) -> None:
    response = admin_api.get("/snuba_datasets")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert set(data) == set(get_enabled_dataset_names())


@pytest.mark.redis_db
def test_snuba_debug_invalid_query(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "transactions", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Rule 'query_exp' didn't match at '' (line 1, column 1)."


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_snuba_debug_valid_query(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (functions)
    SELECT worst
    WHERE project_id IN tuple(100)
    AND timestamp >= toDateTime('2022-01-01 00:00:00')
    AND timestamp < toDateTime('2022-02-01 00:00:00')
    """
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "functions", "query": snql_query})
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["sql"] != ""
    assert "timing" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_snuba_debug_explain_query(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (functions)
    SELECT worst
    WHERE project_id IN tuple(100)
    AND timestamp >= toDateTime('2022-01-01 00:00:00')
    AND timestamp < toDateTime('2022-02-01 00:00:00')
    """
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "functions", "query": snql_query})
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["sql"] != ""
    assert len(data["explain"]["steps"]) > 0
    assert data["explain"]["original_ast"].startswith("SELECT")

    expected_steps = [
        {
            "category": "storage_planning",
            "name": "mappers",
            "type": "query_transform",
        },
        {
            "category": "snql_parsing",
            "name": "_parse_datetime_literals",
            "type": "query_transform",
        },
    ]

    for e in expected_steps:
        assert any(
            step["category"] == e["category"]
            and step["name"] == e["name"]
            and step["type"] == e["type"]
            and step["data"]["original"] != ""
            and step["data"]["transformed"] != ""
            and len(step["data"]["diff"]) > 0
            for step in data["explain"]["steps"]
        )


class FakeRoutingStrategy(BaseRoutingStrategy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [
            Configuration(
                "fake_strategy_config",
                "A fake config for testing",
                int,
                50,
            )
        ]

    def get_allocation_policies(self) -> list[AllocationPolicy]:
        policy = FakePolicy(ResourceIdentifier(self.__class__.__name__), [], {})
        policy.set_config_value("fake_optional_config", 15, {"org_id": 15})
        return [policy]

    def get_delete_allocation_policies(self) -> list[AllocationPolicy]:
        policy = FakeDeletePolicy(ResourceIdentifier(self.__class__.__name__), [], {})
        policy.set_config_value("fake_optional_config", 20, {"org_id": 20})
        return [policy]


@pytest.mark.redis_db
def test_get_routing_strategy_configs(admin_api: FlaskClient) -> None:
    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.BaseRoutingStrategy.get_from_name",
        side_effect=lambda strategy_name: FakeRoutingStrategy,
    ):
        response = admin_api.get("/routing_strategy_configs/FakeRoutingStrategy")

    assert response.status_code == 200
    assert response.json is not None

    strategy_data = response.json

    # Check strategy-level data
    assert strategy_data["configurable_component_class_name"] == "FakeRoutingStrategy"
    assert len(strategy_data["configurations"]) == 2
    assert {
        "name": "some_default_config",
        "type": "int",
        "default": 100,
        "description": "Placeholder for now",
        "value": 100,
        "params": {},
    } in strategy_data["configurations"]
    assert {
        "name": "fake_strategy_config",
        "type": "int",
        "default": 50,
        "description": "A fake config for testing",
        "value": 50,
        "params": {},
    } in strategy_data["configurations"]
    assert strategy_data["optional_config_definitions"] == []

    # Check policies data
    assert len(strategy_data["policies_data"]) == 2

    # First policy
    assert strategy_data["policies_data"][0]["configurable_component_class_name"] == "FakePolicy"
    assert strategy_data["policies_data"][0]["query_type"] == "select"
    assert len(strategy_data["policies_data"][0]["configurations"]) == 4
    assert {
        "name": "fake_optional_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 15,
        "params": {"org_id": 15},
    } in strategy_data["policies_data"][0]["configurations"]
    assert {
        "name": "is_enforced",
        "type": "int",
        "default": 1,
        "description": "Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
        "value": 1,
        "params": {},
    } in strategy_data["policies_data"][0]["configurations"]
    assert {
        "name": "is_active",
        "type": "int",
        "default": 1,
        "description": "Toggles whether or not this policy is active. If active, policy code will be excecuted. If inactive, the policy code will not run and the query will pass through.",
        "value": 1,
        "params": {},
    } in strategy_data["policies_data"][0]["configurations"]
    assert {
        "name": "max_threads",
        "type": "int",
        "default": 10,
        "description": "The max threads Clickhouse can use for the query.",
        "value": 10,
        "params": {},
    } in strategy_data["policies_data"][0]["configurations"]
    assert len(strategy_data["policies_data"][0]["optional_config_definitions"]) == 1
    assert {
        "name": "fake_optional_config",
        "type": "int",
        "default": -1,
        "description": "",
        "params": [{"name": "org_id", "type": "int"}],
    } in strategy_data["policies_data"][0]["optional_config_definitions"]

    # Second policy
    assert (
        strategy_data["policies_data"][1]["configurable_component_class_name"] == "FakeDeletePolicy"
    )
    assert strategy_data["policies_data"][1]["query_type"] == "delete"
    assert len(strategy_data["policies_data"][1]["configurations"]) == 4
    assert {
        "name": "fake_optional_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 20,
        "params": {"org_id": 20},
    } in strategy_data["policies_data"][1]["configurations"]
    assert {
        "name": "is_enforced",
        "type": "int",
        "default": 1,
        "description": "Toggles whether or not this policy is enforced. If enforced, policy will be able to throttle/reject incoming queries. If not enforced, this policy will not throttle/reject queries if policy is triggered, but all the policy code will still run.",
        "value": 1,
        "params": {},
    } in strategy_data["policies_data"][1]["configurations"]
    assert {
        "name": "is_active",
        "type": "int",
        "default": 1,
        "description": "Toggles whether or not this policy is active. If active, policy code will be excecuted. If inactive, the policy code will not run and the query will pass through.",
        "value": 1,
        "params": {},
    } in strategy_data["policies_data"][1]["configurations"]
    assert {
        "name": "max_threads",
        "type": "int",
        "default": 10,
        "description": "The max threads Clickhouse can use for the query.",
        "value": 10,
        "params": {},
    } in strategy_data["policies_data"][1]["configurations"]
    assert len(strategy_data["policies_data"][1]["optional_config_definitions"]) == 1
    assert {
        "name": "fake_optional_config",
        "type": "int",
        "default": -1,
        "description": "",
        "params": [{"name": "org_id", "type": "int"}],
    } in strategy_data["policies_data"][1]["optional_config_definitions"]


class FakePolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [Configuration("fake_optional_config", "", int, -1, param_types={"org_id": int})]

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=True,
            max_threads=1,
            explanation={},
            is_throttled=False,
            rejection_threshold=MAX_THRESHOLD,
            throttle_threshold=MAX_THRESHOLD,
            quota_used=0,
            quota_unit=NO_UNITS,
            suggestion=NO_SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        pass


class FakeDeletePolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[Configuration]:
        return [Configuration("fake_optional_config", "", int, -1, param_types={"org_id": int})]

    def _get_quota_allowance(
        self, tenant_ids: dict[str, str | int], query_id: str
    ) -> QuotaAllowance:
        return QuotaAllowance(
            can_run=True,
            max_threads=1,
            explanation={},
            is_throttled=False,
            rejection_threshold=MAX_THRESHOLD,
            throttle_threshold=MAX_THRESHOLD,
            quota_used=0,
            quota_unit=NO_UNITS,
            suggestion=NO_SUGGESTION,
        )

    def _update_quota_balance(
        self,
        tenant_ids: dict[str, str | int],
        query_id: str,
        result_or_error: QueryResultOrError,
    ) -> None:
        pass

    @property
    def query_type(self) -> QueryType:
        return QueryType.DELETE


@pytest.mark.redis_db
def test_get_allocation_policy_configs(admin_api: FlaskClient) -> None:
    def mock_get_policies() -> list[AllocationPolicy]:
        policy = FakePolicy(ResourceIdentifier(StorageKey("nothing")), [], {})
        policy.set_config_value("fake_optional_config", 10, {"org_id": 10})
        return [policy]

    with mock.patch(
        "snuba.datasets.storage.ReadableTableStorage.get_allocation_policies",
        side_effect=mock_get_policies,
    ):
        response = admin_api.get("/allocation_policy_configs/errors")

    assert response.status_code == 200
    assert response.json is not None and len(response.json) == 1
    [data] = response.json
    assert data["query_type"] == "select"
    assert data["policy_name"] == "FakePolicy"
    assert data["optional_config_definitions"] == [
        {
            "name": "fake_optional_config",
            "type": "int",
            "default": -1,
            "description": "",
            "params": [{"name": "org_id", "type": "int"}],
        }
    ]
    assert {
        "name": "fake_optional_config",
        "type": "int",
        "default": -1,
        "description": "",
        "value": 10,
        "params": {"org_id": 10},
    } in data["configs"]


@pytest.mark.redis_db
def test_set_allocation_policy_config(admin_api: FlaskClient) -> None:
    # an end to end test setting a config, retrieving allocation policy configs,
    # and deleting the config afterwards
    auditlog_records = []

    def mock_record(user: Any, action: Any, data: Any, notify: Any) -> None:
        nonlocal auditlog_records
        auditlog_records.append((user, action, data, notify))

    with mock.patch("snuba.admin.views.audit_log.record", side_effect=mock_record):
        response = admin_api.post(
            "/set_configurable_component_configuration",
            data=json.dumps(
                {
                    "configurable_component_namespace": "AllocationPolicy",
                    "configurable_component_class_name": "BytesScannedWindowAllocationPolicy",
                    "resource_name": "errors",
                    "key": "org_limit_bytes_scanned_override",
                    "params": {"org_id": 1},
                    "value": "420",
                }
            ),
        )

        assert response.status_code == 200, response.json
        # make sure an auditlog entry was recorded
        assert auditlog_records.pop()
        response = admin_api.get("/allocation_policy_configs/errors")
        assert response.status_code == 200

        # three policies
        assert response.json is not None and len(response.json) == 5
        policy_configs = response.json
        bytes_scanned_policy = [
            policy
            for policy in policy_configs
            if policy["policy_name"] == "BytesScannedWindowAllocationPolicy"
        ][0]

        assert bytes_scanned_policy["policy_name"] == "BytesScannedWindowAllocationPolicy"
        assert {
            "default": -1,
            "description": "Number of bytes a specific org can scan in a 10 minute " "window.",
            "name": "org_limit_bytes_scanned_override",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } in bytes_scanned_policy["configs"]

        # no need to record auditlog when nothing was updated
        assert not auditlog_records
        assert (
            admin_api.delete(
                "/set_configurable_component_configuration",
                data=json.dumps(
                    {
                        "configurable_component_namespace": "AllocationPolicy",
                        "configurable_component_class_name": "BytesScannedWindowAllocationPolicy",
                        "resource_name": "errors",
                        "key": "org_limit_bytes_scanned_override",
                        "params": {"org_id": 1},
                    }
                ),
            ).status_code
            == 200
        )

        response = admin_api.get("/allocation_policy_configs/errors")
        assert response.status_code == 200
        assert response.json is not None and len(response.json) == 5
        assert {
            "default": -1,
            "description": "Number of bytes a specific org can scan in a 10 minute " "window.",
            "name": "org_limit_bytes_scanned_override",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } not in response.json[0]["configs"]
        # make sure an auditlog entry was recorded
        assert auditlog_records.pop()


@pytest.mark.redis_db
def test_set_routing_strategy_config(admin_api: FlaskClient) -> None:

    auditlog_records = []

    def mock_record(user: Any, action: Any, data: Any, notify: Any) -> None:
        nonlocal auditlog_records
        auditlog_records.append((user, action, data, notify))

    with (
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.BaseRoutingStrategy.get_from_name",
            side_effect=lambda strategy_name: FakeRoutingStrategy,
        ),
        mock.patch("snuba.admin.views.audit_log.record", side_effect=mock_record),
    ):

        # Set a routing strategy config
        response = admin_api.post(
            "/set_configurable_component_configuration",
            data=json.dumps(
                {
                    "configurable_component_namespace": "BaseRoutingStrategy",
                    "configurable_component_class_name": "FakeRoutingStrategy",
                    "resource_name": "FakeRoutingStrategy",
                    "key": "fake_strategy_config",
                    "value": "75",
                }
            ),
        )
        assert response.status_code == 200, response.json
        assert auditlog_records.pop()

        # Retrieve the routing strategy configs to verify the config was set
        response = admin_api.get("/routing_strategy_configs/FakeRoutingStrategy")
        assert response.status_code == 200
        assert response.json is not None

        # Verify the config was set correctly
        strategy_data = response.json
        assert strategy_data["configurable_component_class_name"] == "FakeRoutingStrategy"
        assert {
            "name": "fake_strategy_config",
            "type": "int",
            "default": 50,
            "description": "A fake config for testing",
            "value": 75,
            "params": {},
        } in strategy_data["configurations"]

        # Delete the routing strategy config
        response = admin_api.delete(
            "/set_configurable_component_configuration",
            data=json.dumps(
                {
                    "configurable_component_namespace": "BaseRoutingStrategy",
                    "configurable_component_class_name": "FakeRoutingStrategy",
                    "resource_name": "FakeRoutingStrategy",
                    "key": "fake_strategy_config",
                }
            ),
        )
        assert response.status_code == 200

        # Verify the config was deleted by checking again
        response = admin_api.get("/routing_strategy_configs/FakeRoutingStrategy")
        assert response.status_code == 200
        assert response.json is not None

        # The config should be back to its default value
        strategy_data = response.json
        assert strategy_data["configurable_component_class_name"] == "FakeRoutingStrategy"
        assert {
            "name": "fake_strategy_config",
            "type": "int",
            "default": 50,
            "description": "A fake config for testing",
            "value": 50,
            "params": {},
        } in strategy_data["configurations"]

        # make sure an auditlog entry was recorded for the delete
        assert auditlog_records.pop()


@pytest.mark.redis_db
def test_set_allocation_policy_config_for_strategy(admin_api: FlaskClient) -> None:
    auditlog_records = []

    def mock_record(user: Any, action: Any, data: Any, notify: Any) -> None:
        nonlocal auditlog_records
        auditlog_records.append((user, action, data, notify))

    def mock_get_from_name(strategy_name: str) -> Type[BaseRoutingStrategy]:
        return FakeRoutingStrategy

    with (
        mock.patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.BaseRoutingStrategy.get_from_name",
            side_effect=mock_get_from_name,
        ),
        mock.patch("snuba.admin.views.audit_log.record", side_effect=mock_record),
    ):

        # Set an allocation policy config for the strategy
        response = admin_api.post(
            "/set_configurable_component_configuration",
            data=json.dumps(
                {
                    "configurable_component_namespace": "AllocationPolicy",
                    "configurable_component_class_name": "FakePolicy",
                    "resource_name": "FakeRoutingStrategy",
                    "key": "fake_optional_config",
                    "params": {"org_id": 1},
                    "value": "420",
                }
            ),
        )
        assert response.status_code == 200, response.json
        # make sure an auditlog entry was recorded
        assert auditlog_records.pop()

        # Retrieve the allocation policy configs to verify the config was set
        response = admin_api.get("/routing_strategy_configs/FakeRoutingStrategy")
        assert response.status_code == 200
        assert response.json is not None

        strategy_data = response.json
        assert strategy_data["configurable_component_class_name"] == "FakeRoutingStrategy"
        assert len(strategy_data["policies_data"]) == 2

        fake_policy = next(
            policy
            for policy in strategy_data["policies_data"]
            if policy["configurable_component_class_name"] == "FakePolicy"
        )

        assert fake_policy["configurable_component_class_name"] == "FakePolicy"
        assert {
            "default": -1,
            "description": "",
            "name": "fake_optional_config",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } in fake_policy["configurations"]

        # Delete the allocation policy config for the strategy
        response = admin_api.delete(
            "/set_configurable_component_configuration",
            data=json.dumps(
                {
                    "configurable_component_namespace": "AllocationPolicy",
                    "configurable_component_class_name": "FakePolicy",
                    "resource_name": "FakeRoutingStrategy",
                    "key": "fake_optional_config",
                    "params": {"org_id": 1},
                }
            ),
        )
        assert response.status_code == 200

        # Verify the config was deleted by checking again
        response = admin_api.get("/routing_strategy_configs/FakeRoutingStrategy")
        assert response.status_code == 200
        assert response.json is not None

        strategy_data = response.json
        fake_policy = next(
            policy
            for policy in strategy_data["policies_data"]
            if policy["configurable_component_class_name"] == "FakePolicy"
        )

        # The config should be back to its default value
        assert {
            "default": -1,
            "description": "",
            "name": "fake_optional_config",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } not in fake_policy["configurations"]

        # make sure an auditlog entry was recorded for the delete
        assert auditlog_records.pop()


@pytest.mark.redis_db
def test_prod_snql_query_invalid_dataset(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/production_snql_query", data=json.dumps({"dataset": "", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "dataset '' does not exist"


@pytest.mark.redis_db
def test_prod_snql_query_invalid_query(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/production_snql_query", data=json.dumps({"dataset": "functions", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Rule 'query_exp' didn't match at '' (line 1, column 1)."


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_force_overwrite(admin_api: FlaskClient) -> None:
    migration_id = "0011_add_timestamp_ms"
    migrations = json.loads(admin_api.get("/migrations/search_issues/list").data)
    downgraded_migration = [m for m in migrations if m.get("migration_id") == migration_id][0]
    assert downgraded_migration["status"] == "completed"

    response = admin_api.post(
        f"/migrations/search_issues/overwrite/{migration_id}/status/not_started",
        headers={"Referer": "https://snuba-admin.getsentry.net/"},
    )
    assert response.status_code == 200
    migrations = json.loads(admin_api.get("/migrations/search_issues/list").data)
    downgraded_migration = [m for m in migrations if m.get("migration_id") == migration_id][0]
    assert downgraded_migration["status"] == "not_started"


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_snql_query_valid_query(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (events)
    SELECT title
    WHERE project_id = 1
    AND timestamp >= toDateTime('2023-01-01 00:00:00')
    AND timestamp < toDateTime('2023-02-01 00:00:00')
    """
    response = admin_api.post(
        "/production_snql_query",
        data=json.dumps({"dataset": "events", "query": snql_query}),
        headers={"Referer": "https://snuba-admin.getsentry.net/"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "data" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_snql_query_multiple_allowed_projects(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (transactions)
    SELECT title
    WHERE project_id IN array(1, 11276)
    AND finish_ts >= toDateTime('2023-01-01 00:00:00')
    AND finish_ts < toDateTime('2023-02-01 00:00:00')
    """
    response = admin_api.post(
        "/production_snql_query",
        data=json.dumps({"dataset": "transactions", "query": snql_query}),
        headers={"Referer": "https://snuba-admin.getsentry.net/"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "data" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_snql_query_invalid_project_query(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (events)
    SELECT title
    WHERE project_id = 2
    AND timestamp >= toDateTime('2023-01-01 00:00:00')
    AND timestamp < toDateTime('2023-02-01 00:00:00')
    """
    response = admin_api.post(
        "/production_snql_query",
        data=json.dumps({"dataset": "events", "query": snql_query}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Cannot access the following project ids: {2}"


@pytest.mark.redis_db
def test_prod_mql_query_invalid_dataset(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/production_mql_query",
        data=json.dumps({"dataset": "", "query": "", "mql_context": {}}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "dataset '' does not exist"


@pytest.mark.redis_db
def test_prod_mql_query_invalid_query(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/production_mql_query",
        data=json.dumps({"dataset": "functions", "query": "", "mql_context": {}}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Rule 'expression' didn't match at '' (line 1, column 1)."


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_mql_query_valid_query(admin_api: FlaskClient) -> None:
    mql_query = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    mql_context = {
        "entity": "generic_metrics_distributions",
        "start": "2023-11-23T18:30:00",
        "end": "2023-11-23T22:30:00",
        "rollup": {
            "granularity": 60,
            "interval": 60,
            "with_totals": "False",
            "orderby": None,
        },
        "scope": {
            "org_ids": [1],
            "project_ids": [1],
            "use_case_id": "transactions",
        },
        "indexer_mappings": {
            "d:transactions/duration@millisecond": 123456,
            "d:transactions/measurements.fp@millisecond": 789012,
            "status_code": 222222,
            "transaction": 333333,
        },
        "limit": None,
        "offset": None,
    }
    response = admin_api.post(
        "/production_mql_query",
        data=json.dumps(
            {
                "dataset": "generic_metrics",
                "query": mql_query,
                "mql_context": mql_context,
            }
        ),
        headers={"Referer": "https://snuba-admin.getsentry.net/"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "data" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_mql_query_multiple_allowed_projects(admin_api: FlaskClient) -> None:
    mql_query = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    mql_context = {
        "entity": "generic_metrics_distributions",
        "start": "2023-11-23T18:30:00",
        "end": "2023-11-23T22:30:00",
        "rollup": {
            "granularity": 60,
            "interval": 60,
            "with_totals": "False",
            "orderby": None,
        },
        "scope": {
            "org_ids": [1],
            "project_ids": [1, 11276],
            "use_case_id": "transactions",
        },
        "indexer_mappings": {
            "d:transactions/duration@millisecond": 123456,
            "d:transactions/measurements.fp@millisecond": 789012,
            "status_code": 222222,
            "transaction": 333333,
        },
        "limit": None,
        "offset": None,
    }
    response = admin_api.post(
        "/production_mql_query",
        data=json.dumps(
            {
                "dataset": "generic_metrics",
                "query": mql_query,
                "mql_context": mql_context,
            }
        ),
        headers={"Referer": "https://snuba-admin.getsentry.net/"},
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "data" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_mql_query_invalid_project_query(admin_api: FlaskClient) -> None:
    mql_query = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    mql_context = {
        "entity": "generic_metrics_distributions",
        "start": "2023-11-23T18:30:00",
        "end": "2023-11-23T22:30:00",
        "rollup": {
            "granularity": 60,
            "interval": 60,
            "with_totals": "False",
            "orderby": None,
        },
        "scope": {
            "org_ids": [1],
            "project_ids": [2],
            "use_case_id": "transactions",
        },
        "indexer_mappings": {
            "d:transactions/duration@millisecond": 123456,
            "d:transactions/measurements.fp@millisecond": 789012,
            "status_code": 222222,
            "transaction": 333333,
        },
        "limit": None,
        "offset": None,
    }
    response = admin_api.post(
        "/production_mql_query",
        data=json.dumps(
            {
                "dataset": "generic_metrics",
                "query": mql_query,
                "mql_context": mql_context,
            }
        ),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Cannot access the following project ids: {2}"


@mock.patch("snuba.admin.clickhouse.database_clusters.get_ro_node_connection")
@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_clickhouse_node_info(
    get_ro_node_connection_mock: mock.Mock, admin_api: FlaskClient
) -> None:
    expected_result = {
        "cluster": "Cluster",
        "host_name": "Host",
        "host_address": "127.0.0.1",
        "port": 9000,
        "shard": 1,
        "replica": 1,
        "version": "v1",
    }

    connection_mock = mock.Mock()
    connection_mock.execute.return_value = FakeClickhouseResult(
        [("Cluster", "Host", "127.0.0.1", 9000, 1, 1, "v1")]
    )
    get_ro_node_connection_mock.return_value = connection_mock
    response = admin_api.get("/clickhouse_node_info")
    assert response.status_code == 200

    response_data = json.loads(response.data)
    assert (
        len(response_data) > 0
        and {k: response_data[0][k] for k in expected_result.keys()} == expected_result
    )


@mock.patch("snuba.admin.clickhouse.database_clusters.get_ro_node_connection")
@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_clickhouse_system_settings(
    get_ro_node_connection_mock: mock.Mock, admin_api: FlaskClient
) -> None:
    expected_result = [
        {
            "name": "max_memory_usage",
            "value": "10000000000",
            "default": "10000000000",
            "changed": 0,
            "description": "Maximum memory usage for query execution",
            "type": "UInt64",
        },
        {
            "name": "max_threads",
            "value": "8",
            "default": "8",
            "changed": 0,
            "description": "The maximum number of threads to execute the request",
            "type": "UInt64",
        },
    ]

    connection_mock = mock.Mock()
    connection_mock.execute.return_value = FakeClickhouseResult(
        [
            (
                "max_memory_usage",
                "10000000000",
                "10000000000",
                0,
                "Maximum memory usage for query execution",
                "UInt64",
            ),
            (
                "max_threads",
                "8",
                "8",
                0,
                "The maximum number of threads to execute the request",
                "UInt64",
            ),
        ]
    )
    get_ro_node_connection_mock.return_value = connection_mock

    response = admin_api.get(
        "/clickhouse_system_settings?host=test_host&port=9000&storage=test_storage"
    )
    assert response.status_code == 200

    response_data = json.loads(response.data)
    assert response_data == expected_result

    # Test error case when parameters are missing
    response = admin_api.get("/clickhouse_system_settings")
    assert response.status_code == 400
    assert json.loads(response.data) == {"error": "Host, port, and storage are required"}


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_execute_rpc_endpoint_success(
    admin_api: FlaskClient,
    rpc_test_setup: Tuple[Type[Any], Type[RPCEndpoint[Any, TimeSeriesResponse]]],
) -> None:
    MyRequest, TestRPC = rpc_test_setup

    start_time = BASE_TIME - timedelta(minutes=30)
    end_time = BASE_TIME + timedelta(hours=24)

    payload = json.dumps(
        {
            "meta": {
                "organization_id": 123,
                "project_ids": [1],
                "start_timestamp": start_time.isoformat(),
                "end_timestamp": end_time.isoformat(),
                "trace_item_type": 1,
                "referrer": "test_referrer",
                "request_id": "test_request_id",
            },
        }
    )

    response = admin_api.post(
        "/rpc_execute/TestRPC/v1",
        data=payload,
        content_type="application/json",
    )

    assert response.status_code == 200
    response_data = json.loads(response.data)
    assert response_data == {}


@pytest.mark.redis_db
def test_execute_rpc_endpoint_unknown_endpoint(admin_api: FlaskClient) -> None:
    payload = json.dumps(
        {
            "message": "test_execute_rpc_endpoint_unknown_endpoint",
            "meta": {
                "organization_id": 123,
                "project_ids": [1],
            },
        }
    )

    response = admin_api.post(
        "/rpc_execute/UnknownRPC/v1",
        data=payload,
        content_type="application/json",
    )
    assert response.status_code == 404
    assert json.loads(response.data) == {"error": "Unknown endpoint: UnknownRPC or version: v1"}


@pytest.mark.redis_db
def test_execute_rpc_endpoint_invalid_payload(
    admin_api: FlaskClient,
    rpc_test_setup: Tuple[Type[Any], Type[RPCEndpoint[Any, TimeSeriesResponse]]],
) -> None:
    MyRequest, TestRPC = rpc_test_setup

    invalid_payload = json.dumps({"invalid": "data"})
    response = admin_api.post(
        "/rpc_execute/TestRPC/v1",
        data=invalid_payload,
        content_type="application/json",
    )
    assert response.status_code == 400
    assert "error" in json.loads(response.data)


@pytest.mark.redis_db
def test_execute_rpc_endpoint_org_id_not_allowed(
    admin_api: FlaskClient,
    rpc_test_setup: Tuple[Type[Any], Type[RPCEndpoint[Any, TimeSeriesResponse]]],
) -> None:
    MyRequest, TestRPC = rpc_test_setup

    payload = json.dumps(
        {
            "meta": {
                "organization_id": 999999,
                "project_ids": [1],
            },
        }
    )

    response = admin_api.post(
        "/rpc_execute/TestRPC/v1",
        data=payload,
        content_type="application/json",
    )

    assert response.status_code == 400
    response_data = json.loads(response.data)
    assert "error" in response_data
    assert "Organization ID 999999 is not allowed" in response_data["error"]


@pytest.mark.redis_db
def test_list_rpc_endpoints(admin_api: FlaskClient) -> None:
    response = admin_api.get("/rpc_endpoints")
    assert response.status_code == 200

    endpoint_names = json.loads(response.data)
    assert isinstance(endpoint_names, list)
    assert len(endpoint_names) > 0

    for endpoint in endpoint_names:
        assert isinstance(endpoint, list)
        assert len(endpoint) == 2
        assert isinstance(endpoint[0], str)
        assert isinstance(endpoint[1], str)

    registered_endpoints = {tuple(name.split("__")) for name in RPCEndpoint.all_names()}
    response_endpoints = set(tuple(endpoint) for endpoint in endpoint_names)
    assert response_endpoints == registered_endpoints
