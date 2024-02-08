from __future__ import annotations

from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.admin.auth_roles import ROLES


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


@pytest.mark.redis_db
def test_tools(admin_api: FlaskClient) -> None:
    response = admin_api.get("/tools")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data["tools"]) > 0
    assert "snql-to-sql" in data["tools"]
    assert "all" in data["tools"]


@pytest.mark.redis_db
@patch("snuba.admin.auth.DEFAULT_ROLES", [ROLES["ProductTools"]])
def test_product_tools_role(
    admin_api: FlaskClient,
) -> None:
    response = admin_api.get("/tools")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data["tools"]) > 0
    assert "snql-to-sql" in data["tools"]
    assert "tracing" in data["tools"]
    assert "system-queries" in data["tools"]
    assert "production-queries" in data["tools"]
    assert "all" not in data["tools"]
