from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest
import simplejson as json
from flask.testing import FlaskClient
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    TimeSeriesRequest,
    TimeSeriesResponse,
)

from snuba.admin.auth import USER_HEADER_KEY
from snuba.datasets.factory import get_enabled_dataset_names
from snuba.web.rpc import RPCEndpoint

BASE_TIME = datetime.utcnow().replace(
    hour=8, minute=0, second=0, microsecond=0, tzinfo=UTC
) - timedelta(hours=24)


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


@pytest.fixture(scope="session")
def rpc_test_setup() -> tuple[type[Any], type[RPCEndpoint[Any, TimeSeriesResponse]]]:
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
@pytest.mark.events_db
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
def test_run_copy_table_query_invalid_node_returns_400(admin_api: FlaskClient) -> None:
    """
    Regression for EAP-488 follow-up: the clusterless connection helper now
    raises InvalidNodeError for an attacker-supplied host, and the endpoint
    must surface that as a 400 (like /run_clickhouse_system_query) rather
    than letting it bubble into a 500.
    """
    from snuba.admin.clickhouse.common import InvalidNodeError

    with mock.patch(
        "snuba.admin.views.copy_tables",
        side_effect=InvalidNodeError("host attacker.example.com not in cluster"),
    ):
        response = admin_api.post(
            "/run_copy_table_query",
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "storage": "errors",
                    "source_host": "attacker.example.com",
                }
            ),
        )
    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["error"]["type"] == "request"
    assert "attacker.example.com" in data["error"]["message"]


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
@pytest.mark.events_db
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
@pytest.mark.events_db
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
@pytest.mark.events_db
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
    assert data["error"]["type"] == "clickhouse"


@pytest.mark.redis_db
@pytest.mark.events_db
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
    assert data["error"]["type"] == "validation"


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


@pytest.mark.redis_db
def test_prod_snql_query_invalid_dataset(admin_api: FlaskClient) -> None:
    response = admin_api.post(
        "/production_snql_query", data=json.dumps({"dataset": "", "query": ""})
    )
    assert response.status_code == 404
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
@pytest.mark.events_db
def test_force_overwrite(admin_api: FlaskClient) -> None:
    migration_id = "0012_add_group_id_bloom_filter_index"
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
@pytest.mark.events_db
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
@pytest.mark.events_db
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
@pytest.mark.events_db
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
def test_execute_rpc_endpoint_success(
    admin_api: FlaskClient,
    rpc_test_setup: tuple[type[Any], type[RPCEndpoint[Any, TimeSeriesResponse]]],
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
    rpc_test_setup: tuple[type[Any], type[RPCEndpoint[Any, TimeSeriesResponse]]],
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
    rpc_test_setup: tuple[type[Any], type[RPCEndpoint[Any, TimeSeriesResponse]]],
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
    response_endpoints = {tuple(endpoint) for endpoint in endpoint_names}
    assert response_endpoints == registered_endpoints


@pytest.mark.redis_db
def test_uncaught_exception_returns_json_500(admin_api: FlaskClient) -> None:
    # The /tools endpoint has no try/except; make its handler raise an
    # unexpected error and confirm the global handler repackages it as JSON.
    with mock.patch(
        "snuba.admin.views.get_user_allowed_tools",
        side_effect=RuntimeError("boom"),
    ):
        response = admin_api.get("/tools")

    assert response.status_code == 500
    assert response.headers["Content-Type"] == "application/json"
    assert json.loads(response.data) == {"error": {"type": "unknown", "message": "boom"}}


@pytest.mark.redis_db
def test_http_exception_passes_through(admin_api: FlaskClient) -> None:
    # The catch-all must not swallow werkzeug HTTPExceptions into a 500.
    response = admin_api.get("/this_route_does_not_exist")
    assert response.status_code == 404


@pytest.mark.redis_db
def test_get_job_types_lists_only_adhoc_allowed(admin_api: FlaskClient) -> None:
    response = admin_api.get("/job-types")
    assert response.status_code == 200
    job_types = json.loads(response.data)
    assert isinstance(job_types, list)
    # Read-only / idempotent jobs opt in and are runnable without a manifest.
    assert "ToyJob" in job_types
    assert "LogRuntimeConfigs" in job_types
    # Destructive jobs stay gated behind a manifest entry.
    assert "DeleteEventsByTagKeyValue" not in job_types
    assert "ScrubIpFromEAPSpans" not in job_types


@pytest.mark.redis_db
def test_run_job_by_type_is_repeatable(admin_api: FlaskClient) -> None:
    job_ids = set()
    for _ in range(2):
        response = admin_api.post("/job-types/ToyJob/run")
        assert response.status_code == 200
        body = json.loads(response.data)
        assert body["status"] == "finished"
        assert body["job_id"].startswith("ToyJob_")
        job_ids.add(body["job_id"])
    # A fresh job id per run is what makes it repeatable.
    assert len(job_ids) == 2


@pytest.mark.redis_db
def test_run_job_by_type_rejects_non_adhoc_job(admin_api: FlaskClient) -> None:
    response = admin_api.post("/job-types/DeleteEventsByTagKeyValue/run")
    assert response.status_code == 403
    assert "error" in json.loads(response.data)


@pytest.mark.redis_db
def test_run_job_by_type_unknown_type_returns_403(admin_api: FlaskClient) -> None:
    response = admin_api.post("/job-types/NotARealJob/run")
    assert response.status_code == 403
    assert "error" in json.loads(response.data)
