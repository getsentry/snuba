from __future__ import annotations

from typing import Any
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


def test_list_migration_status(admin_api: FlaskClient) -> None:
    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", {"system", "generic_metrics"}
    ):
        response = admin_api.get("/migrations/system/list")
        assert response.status_code == 200
        expected_json = [
            {
                "blocking": False,
                "migration_id": "0001_migrations",
                "status": "completed",
            }
        ]
        assert json.loads(response.data) == expected_json

        # invalid migration group
        response = admin_api.get("/migrations/sessions/list")
        assert response.status_code == 400
        assert json.loads(response.data)["error"] == "Group not allowed"

        response = admin_api.get("/migrations/bad_group/list")
        assert response.status_code == 400

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", {"sessions", "generic_metrics"}
    ):
        response = admin_api.get("/migrations/sessions/list")
    assert response.status_code == 200
    expected_json = [
        {"blocking": False, "migration_id": "0001_sessions", "status": "completed"},
        {
            "blocking": False,
            "migration_id": "0002_sessions_aggregates",
            "status": "completed",
        },
        {
            "blocking": False,
            "migration_id": "0003_sessions_matview",
            "status": "completed",
        },
    ]

    def sort_by_migration_id(migration: Any) -> Any:
        return migration["migration_id"]

    sorted_response = sorted(json.loads(response.data), key=sort_by_migration_id)
    sorted_expected_json = sorted(expected_json, key=sort_by_migration_id)

    assert sorted_response == sorted_expected_json
