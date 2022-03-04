from __future__ import annotations

from typing import Any

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba import state


@pytest.fixture
def admin_api() -> FlaskClient[Any]:
    from snuba.admin.views import application

    return application.test_client()


def test_get_configs(admin_api: FlaskClient[Any]) -> None:
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


def test_post_configs(admin_api: FlaskClient[Any]) -> None:
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


def test_delete_configs(admin_api: FlaskClient[Any]) -> None:
    # delete a config and its description
    state.set_config("delete_this", "1")
    state.set_config_description("delete_this", "description for this config")
    assert state.get_uncached_config("delete_this") == 1
    assert state.get_config_description("delete_this") == "description for this config"

    response = admin_api.delete("/configs/delete_this")

    assert response.status_code == 200
    assert state.get_uncached_config("delete_this") is None
    assert state.get_config_description("delete_this") is None

    # delete a config but not description
    state.set_config("delete_this", "1")
    state.set_config_description("delete_this", "description for this config")
    assert state.get_uncached_config("delete_this") == 1
    assert state.get_config_description("delete_this") == "description for this config"

    response = admin_api.delete("/configs/delete_this?keepDescription=true")

    assert response.status_code == 200
    assert state.get_uncached_config("delete_this") is None
    assert state.get_config_description("delete_this") == "description for this config"


def test_config_descriptions(admin_api: FlaskClient[Any]) -> None:
    state.set_config_description("desc_test", "description test")
    state.set_config_description("another_test", "another description")
    response = admin_api.get("/all_config_descriptions")
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "desc_test": "description test",
        "another_test": "another description",
    }


def get_node_for_table(
    admin_api: FlaskClient[Any], storage_name: str
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


def test_system_query(admin_api: FlaskClient[Any]) -> None:
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


def test_query_trace(admin_api: FlaskClient[Any]) -> None:
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


def test_query_trace_bad_query(admin_api: FlaskClient[Any]) -> None:
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


def test_query_trace_invalid_query(admin_api: FlaskClient[Any]) -> None:
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
