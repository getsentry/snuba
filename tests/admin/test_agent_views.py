from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.admin.auth import _set_roles, get_iam_roles_from_user
from snuba.admin.auth_roles import DEFAULT_ROLES, ROLES
from snuba.admin.user import AdminUser
from snuba.clickhouse.pool import ClickhouseResult
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


def _post_query(admin_api: FlaskClient, body: object) -> tuple[int, dict[str, Any]]:
    response = admin_api.post(
        "/agent/query",
        headers={"Content-Type": "application/json"},
        data=json.dumps(body),
    )
    return response.status_code, json.loads(response.data)


def test_iam_policy_matches_service_account_members(tmp_path: Path) -> None:
    email = "junior@project.iam.gserviceaccount.com"
    policy = tmp_path / "policy.json"
    policy.write_text(
        json.dumps(
            {"bindings": [{"members": [f"serviceAccount:{email}"], "role": "roles/AgentQueries"}]}
        )
    )
    with patch("snuba.settings.ADMIN_IAM_POLICY_FILE", str(policy)):
        assert get_iam_roles_from_user(AdminUser(email=email, id="1")) == ["AgentQueries"]


@pytest.mark.redis_db
@patch("snuba.admin.auth.get_iam_roles_from_user", return_value=["AgentQueries"])
def test_bound_service_account_gets_only_bound_roles(_: object) -> None:
    user = _set_roles(AdminUser(email="junior@project.iam.gserviceaccount.com", id="1"))
    assert user.roles == [ROLES["AgentQueries"]]


@pytest.mark.redis_db
@patch("snuba.admin.auth.get_iam_roles_from_user", return_value=[])
def test_unbound_service_account_keeps_default_roles(_: object) -> None:
    user = _set_roles(AdminUser(email="other@project.iam.gserviceaccount.com", id="1"))
    assert user.roles == DEFAULT_ROLES


@pytest.mark.redis_db
@patch("snuba.admin.auth.get_iam_roles_from_user", return_value=["AgentQueries"])
def test_person_keeps_default_roles(_: object) -> None:
    user = _set_roles(AdminUser(email="person@sentry.io", id="1"))
    assert user.roles == [ROLES["AgentQueries"], *DEFAULT_ROLES]


@pytest.mark.redis_db
@patch("snuba.admin.auth.DEFAULT_ROLES", [ROLES["ProductTools"]])
def test_agent_query_requires_agent_tool(admin_api: FlaskClient) -> None:
    status, _ = _post_query(admin_api, {"target": "querylog", "sql": "SELECT 1"})
    assert status == 403


@pytest.mark.redis_db
@pytest.mark.parametrize(
    "body, message",
    [
        ([], "Expected a JSON object body"),
        ({"target": "querylog"}, "Missing sql"),
        ({"target": "nope", "sql": "SELECT 1"}, "target must be 'system' or 'querylog'"),
        ({"target": "system", "sql": "SELECT 1"}, "system queries need a storage"),
        ({"target": "system", "storage": "nope", "sql": "SELECT 1"}, "Unknown storage nope"),
    ],
)
def test_agent_query_rejects_bad_requests(
    admin_api: FlaskClient, body: object, message: str
) -> None:
    status, data = _post_query(admin_api, body)
    assert status == 400
    assert str(data["error"]).startswith(message)


@pytest.mark.redis_db
def test_agent_system_query_never_sudo_and_truncates(admin_api: FlaskClient) -> None:
    result = ClickhouseResult(
        results=[[i, ["a"]] for i in range(1001)],
        meta=[("n", "UInt64"), ("arr", "Array(String)")],
    )
    with (
        patch(
            "snuba.admin.agent_views.get_query_node",
            return_value={"host": "query-node", "port": 9000},
        ),
        patch(
            "snuba.admin.agent_views.run_system_query_on_host_with_sql",
            return_value=result,
        ) as run,
    ):
        status, data = _post_query(
            admin_api,
            {"target": "system", "storage": "errors", "sql": "SELECT 1", "sudo": True},
        )

    assert status == 200
    args, kwargs = run.call_args
    assert args[:4] == ("query-node", 9000, "errors", "SELECT 1")
    assert kwargs["sudo_mode"] is False and kwargs["clusterless_mode"] is False
    assert data["column_names"] == ["n", "arr"]
    assert data["rows"][0] == [0, ["a"]]
    assert len(data["rows"]) == 1000 and data["truncated"] is True


@pytest.mark.redis_db
@pytest.mark.events_db
def test_agent_system_query(admin_api: FlaskClient) -> None:
    status, data = _post_query(
        admin_api,
        {
            "target": "system",
            "storage": "errors_ro",
            "sql": "SELECT count() FROM system.replication_queue",
        },
    )
    assert status == 200, data
    assert data == {"column_names": ["count()"], "rows": [[0]], "truncated": False}


@pytest.mark.redis_db
@pytest.mark.events_db
def test_agent_system_query_rejects_non_system_tables(admin_api: FlaskClient) -> None:
    status, data = _post_query(
        admin_api,
        {"target": "system", "storage": "errors_ro", "sql": "SELECT count() FROM errors_local"},
    )
    assert status == 400
    assert "error" in data


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_agent_querylog_query(admin_api: FlaskClient) -> None:
    schema = get_storage(StorageKey.QUERYLOG).get_schema()
    assert isinstance(schema, TableSchema)
    status, data = _post_query(
        admin_api, {"target": "querylog", "sql": f"SELECT count() FROM {schema.get_table_name()}"}
    )
    assert status == 200, data
    assert data["column_names"] == ["count()"]
    assert data["truncated"] is False


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_agent_querylog_rejects_other_tables(admin_api: FlaskClient) -> None:
    status, data = _post_query(
        admin_api, {"target": "querylog", "sql": "SELECT count() FROM errors_local"}
    )
    assert status == 400
    assert str(data["error"]).startswith("Invalid FROM")
