from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.migrations.groups import get_group_loader
from snuba.migrations.runner import MigrationKey, Runner


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
        {"blocking": False, "migration_id": "0004_sessions_ttl", "status": "completed"},
    ]

    def sort_by_migration_id(migration: Any) -> Any:
        return migration["migration_id"]

    sorted_response = sorted(json.loads(response.data), key=sort_by_migration_id)
    sorted_expected_json = sorted(expected_json, key=sort_by_migration_id)

    assert sorted_response == sorted_expected_json


@pytest.mark.parametrize("action", ["run", "reverse"])
def test_run_reverse_migrations(admin_api: FlaskClient, action: str) -> None:

    method = "run_migration" if action == "run" else "reverse_migration"

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", {"system", "invalid_group"}
    ):
        # invalid action
        response = admin_api.post("/migrations/system/invalid_action/0001_migrations/")
        assert response.status_code == 404

        # invalid migration group
        response = admin_api.post(f"/migrations/invalid_group/{action}/0001_migrations")
        assert response.status_code == 400
        assert json.loads(response.data) == {"error": "Group not found"}

        with patch.object(Runner, method) as mock_run_migration:
            response = admin_api.post(f"/migrations/system/{action}/0001_migrations")
            assert response.status_code == 200
            migration_key = MigrationKey(group="system", migration_id="0001_migrations")
            mock_run_migration.assert_called_once_with(
                migration_key, force=False, fake=False, dry_run=False
            )

        with patch.object(Runner, method) as mock_run_migration:
            # not allowed migration group
            response = admin_api.post(f"/migrations/sessions/{action}/0001_sessions")
            assert response.status_code == 400
            assert json.loads(response.data) == {"error": "Group not allowed"}
            assert mock_run_migration.call_count == 0

            # forced migration
            response = admin_api.post(
                f"/migrations/system/{action}/0001_migrations?force=1"
            )
            assert response.status_code == 200
            mock_run_migration.assert_called_once_with(
                migration_key, force=True, fake=False, dry_run=False
            )

        with patch.object(Runner, method) as mock_run_migration:
            # fake migration
            response = admin_api.post(
                f"/migrations/system/{action}/0001_migrations?fake=1&force=true&dry_run=no"
            )
            assert response.status_code == 200
            mock_run_migration.assert_called_once_with(
                migration_key, force=True, fake=True, dry_run=False
            )

        with patch.object(Runner, method) as mock_run_migration:
            # test dry run
            def print_something(*args: Any, **kwargs: Any) -> None:
                print("a dry run")

            mock_run_migration.side_effect = print_something
            response = admin_api.post(
                f"/migrations/system/{action}/0001_migrations?fake=0&force=false&dry_run=yes"
            )
            assert response.status_code == 200
            assert json.loads(response.data) == {"stdout": "a dry run\n"}
            mock_run_migration.assert_called_once_with(
                migration_key, force=False, fake=False, dry_run=True
            )
