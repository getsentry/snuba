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


def test_query_trace(admin_api: Any) -> None:
    response = admin_api.post(
        "/clickhouse_trace_query",
        data=json.dumps(
            {
                "host": "localhost",
                "port": 9000,
                "storage": "errors_ro",
                "sql": "SELECT count() FROM errors_local",
            }
        ),
    )
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "<Debug> executeQuery" in data["trace_output"]
