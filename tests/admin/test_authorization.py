from __future__ import annotations

import pytest
import simplejson as json
from flask.testing import FlaskClient


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
