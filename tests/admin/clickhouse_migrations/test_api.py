from __future__ import annotations

from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.migrations.groups import get_group_loader


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


def test_migration_groups(admin_api: FlaskClient) -> None:
    with patch("snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", {}):
        response = admin_api.get("/migrations/groups")

    assert response.status_code == 200
    assert json.loads(response.data) == []

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", {"system", "generic_metrics"}
    ):
        response = admin_api.get("/migrations/groups")

    assert response.status_code == 200
    assert json.loads(response.data) == [
        {
            "group": "system",
            "migration_ids": get_group_loader("system").get_migrations(),
        },
        {
            "group": "generic_metrics",
            "migration_ids": get_group_loader("generic_metrics").get_migrations(),
        },
    ]
