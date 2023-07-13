from __future__ import annotations

from typing import Any
from unittest import mock

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba import state
from snuba.admin.auth import USER_HEADER_KEY
from snuba.datasets.factory import get_enabled_dataset_names
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QueryResultOrError,
    QuotaAllowance,
)

prod_invalid_query_test_data = [
    pytest.param("SELECT count FROM errors_ro WHERE project_id < 5"),
    pytest.param(
        "SELECT count FROM errors_ro WHERE (project_id > 1 OR (project_id = 1 AND project_id = 2))"
    ),
    pytest.param("SELECT count FROM errors_ro WHERE project_id = 5"),
    pytest.param("SELECT count FROM errors_ro WHERE in((project_id AS a), [1, 2, 3])"),
    pytest.param("SELECT count FROM errors_ro WHERE in(project_id, [2])"),
    pytest.param("SELECT count FROM errors_ro WHERE equals(project_id, 2)"),
    pytest.param("SELECT count FROM errors_ro WHERE equals((project_id AS a), 2)"),
]

prod_valid_query_test_data = [
    pytest.param("SELECT count FROM errors_ro WHERE project_id = 1"),
    pytest.param("SELECT count FROM errors_ro WHERE in((project_id AS a), [1])"),
    pytest.param("SELECT count FROM errors_ro WHERE in(project_id, [1])"),
    pytest.param("SELECT count FROM errors_ro WHERE equals(project_id, 1)"),
    pytest.param("SELECT count FROM errors_ro WHERE equals((project_id AS a), 1)"),
]


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


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
        data=json.dumps(
            {"key": "test_float", "value": "0.1", "description": "test float"}
        ),
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
        data=json.dumps(
            {"key": "test_string", "value": "foo", "description": "test string"}
        ),
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
        data=json.dumps(
            {"key": "test_string", "value": "bar", "description": "test string 2"}
        ),
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
    state.set_config_description(
        "delete/with/slash", "description for delete with slash config"
    )
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
def get_node_for_table(
    admin_api: FlaskClient, storage_name: str
) -> tuple[str, str, int]:
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
def test_query_trace(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {"storage": "errors_ro", "sql": f"SELECT count() FROM {table}"}
        ),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "<Debug> executeQuery" in data["trace_output"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_trace_bad_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {"storage": "errors_ro", "sql": f"SELECT count(asdasds) FROM {table}"}
        ),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert "Exception: Missing columns" in data["error"]["message"]
    assert "clickhouse" == data["error"]["type"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_query_trace_invalid_query(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/clickhouse_trace_query",
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {"storage": "errors_ro", "sql": f"SELECT count() FROM {table};"}
        ),
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
def test_snuba_debug_invalid_dataset(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "dataset '' does not exist"


@pytest.mark.redis_db
def test_snuba_debug_invalid_query(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "sessions", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert (
        data["error"]["message"]
        == "Rule 'query_exp' didn't match at '' (line 1, column 1)."
    )


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_snuba_debug_valid_query(admin_api: FlaskClient) -> None:
    snql_query = """
    MATCH (sessions)
    SELECT sessions_crashed
    WHERE org_id = 100
    AND project_id IN tuple(100)
    AND started >= toDateTime('2022-01-01 00:00:00')
    AND started < toDateTime('2022-02-01 00:00:00')
    """
    response = admin_api.post(
        "/snuba_debug", data=json.dumps({"dataset": "sessions", "query": snql_query})
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["sql"] != ""
    assert len(data["explain"]["steps"]) > 0
    assert {
        "category": "entity_processor",
        "name": "BasicFunctionsProcessor",
        "data": {},
    } in data["explain"]["steps"]


@pytest.mark.redis_db
def test_get_allocation_policy_configs(admin_api: FlaskClient) -> None:
    class FakePolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
            return [
                AllocationPolicyConfig(
                    "fake_optional_config", "", int, -1, param_types={"org_id": int}
                )
            ]

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int]
        ) -> QuotaAllowance:
            return QuotaAllowance(True, 1, {})

        def _update_quota_balance(
            self, tenant_ids: dict[str, str | int], result_or_error: QueryResultOrError
        ) -> None:
            pass

    def mock_get_policies() -> list[AllocationPolicy]:
        policy = FakePolicy(StorageKey("nothing"), [], {})
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
            "/allocation_policy_config",
            data=json.dumps(
                {
                    "storage": "errors",
                    "policy": "BytesScannedWindowAllocationPolicy",
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

        # one policy
        assert response.json is not None and len(response.json) == 1
        [policy_configs] = response.json
        assert policy_configs["policy_name"] == "BytesScannedWindowAllocationPolicy"
        assert {
            "default": -1,
            "description": "Number of bytes a specific org can scan in a 10 minute "
            "window.",
            "name": "org_limit_bytes_scanned_override",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } in policy_configs["configs"]

        # no need to record auditlog when nothing was updated
        assert not auditlog_records
        assert (
            admin_api.delete(
                "/allocation_policy_config",
                data=json.dumps(
                    {
                        "storage": "errors",
                        "policy": "BytesScannedWindowAllocationPolicy",
                        "key": "org_limit_bytes_scanned_override",
                        "params": {"org_id": 1},
                    }
                ),
            ).status_code
            == 200
        )

        response = admin_api.get("/allocation_policy_configs/errors")
        assert response.status_code == 200
        assert response.json is not None and len(response.json) == 1
        assert {
            "default": -1,
            "description": "Number of bytes a specific org can scan in a 10 minute "
            "window.",
            "name": "org_limit_bytes_scanned_override",
            "params": {"org_id": 1},
            "type": "int",
            "value": 420,
        } not in response.json[0]["configs"]
        # make sure an auditlog entry was recorded
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
        "/production_snql_query", data=json.dumps({"dataset": "sessions", "query": ""})
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert (
        data["error"]["message"]
        == "Rule 'query_exp' didn't match at '' (line 1, column 1)."
    )


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
@pytest.mark.clickhouse_db
@pytest.mark.parametrize("query", prod_valid_query_test_data)
def test_prod_sql_query(admin_api: FlaskClient, query: str) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/production_sql_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps({"sql": query}),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "column_names" in data and data["column_names"] == ["count()"]


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_prod_sql_query_missing_project(admin_api: FlaskClient) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/production_sql_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps({"sql": f"SELECT count() FROM {table}"}),
    )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["message"] == "Missing condition on project_id"


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
@pytest.mark.parametrize("query", prod_invalid_query_test_data)
def test_prod_sql_query_invalid_query(admin_api: FlaskClient, query: str) -> None:
    table, _, _ = get_node_for_table(admin_api, "errors_ro")
    response = admin_api.post(
        "/production_sql_query",
        headers={"Content-Type": "application/json", USER_HEADER_KEY: "test"},
        data=json.dumps({"sql": query}),
    )
    assert response.status_code == 400
