from __future__ import annotations

from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba import settings
from snuba.admin.tool_policies import DEVELOPER_TOOLS


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


def test_tools(admin_api: FlaskClient) -> None:
    response = admin_api.get("/tools")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data["tools"]) > 0
    assert "snql-to-sql" in data["tools"]
    assert "all" in data["tools"]


@patch("snuba.settings.ADMIN_DEVELOPER_MODE", True)
def test_tools_developer_mode(admin_api: FlaskClient) -> None:
    response = admin_api.get("/tools")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data["tools"]) == len(DEVELOPER_TOOLS)
    assert set(data["tools"]) == set(t.value for t in DEVELOPER_TOOLS)
    assert "all" not in data["tools"]


@patch("snuba.settings.ADMIN_DEVELOPER_MODE", True)
def test_invalid_request_developer_mode(admin_api: FlaskClient) -> None:
    settings.ADMIN_DEVELOPER_MODE = True
    response = admin_api.get("/clickhouse_queries")
    assert response.status_code == 403
    data = json.loads(response.data)
    assert "No permissions" in data["error"]
