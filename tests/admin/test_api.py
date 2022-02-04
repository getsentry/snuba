from __future__ import annotations

from typing import Any

import pytest
import simplejson as json

from snuba import state


@pytest.fixture
def admin_api() -> Any:
    from snuba.admin.views import application

    return application.test_client()


def test_get_configs(admin_api: Any) -> None:
    response = admin_api.get("/configs")
    assert response.status_code == 200
    assert json.loads(response.data) == []

    # Add string config
    state.set_config("cfg1", "hello world")

    # Add int config
    state.set_config("cfg2", "12")

    # Add float config
    state.set_config("cfg3", "1.0")

    response = admin_api.get("/configs")
    assert response.status_code == 200
    assert json.loads(response.data) == [
        {"key": "cfg1", "type": "string", "value": "hello world"},
        {"key": "cfg2", "type": "int", "value": "12"},
        {"key": "cfg3", "type": "float", "value": "1.0"},
    ]


def test_post_configs(admin_api: Any) -> None:
    # int
    response = admin_api.post(
        "/configs", data=json.dumps({"key": "test_int", "value": "1"})
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {"key": "test_int", "value": "1", "type": "int"}

    # float
    response = admin_api.post(
        "/configs", data=json.dumps({"key": "test_float", "value": "0.1"})
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "key": "test_float",
        "value": "0.1",
        "type": "float",
    }

    # string
    response = admin_api.post(
        "/configs", data=json.dumps({"key": "test_string", "value": "foo"})
    )
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "key": "test_string",
        "value": "foo",
        "type": "string",
    }

    # reject duplicate key
    response = admin_api.post(
        "/configs", data=json.dumps({"key": "test_string", "value": "bar"})
    )
    assert response.status_code == 400


def get_node_for_table(admin_api: Any, storage_name: str) -> tuple[str, str, int]:
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


def test_system_query(admin_api: Any) -> None:
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


def test_query_trace(admin_api: Any) -> None:
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


def test_query_trace_bad_query(admin_api: Any) -> None:
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


def test_query_trace_invalid_query(admin_api: Any) -> None:
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
