from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.migrations.groups import MigrationGroup, get_group_loader
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


def test_migration_groups(admin_api: FlaskClient) -> None:
    with patch("snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS", dict()):
        response = admin_api.get("/migrations/groups")

    assert response.status_code == 200
    assert json.loads(response.data) == []

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS",
        {"system": "AllMigrationsPolicy", "generic_metrics": "NoMigrationsPolicy"},
    ):
        response = admin_api.get("/migrations/groups")

    assert response.status_code == 200
    assert json.loads(response.data) == [
        {
            "group": "system",
            "migration_ids": get_group_loader(MigrationGroup.SYSTEM).get_migrations(),
        },
        {
            "group": "generic_metrics",
            "migration_ids": get_group_loader(
                MigrationGroup.GENERIC_METRICS
            ).get_migrations(),
        },
    ]


def test_list_migration_status(admin_api: FlaskClient) -> None:
    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS",
        {
            "system": "NonBlockingMigrationsPolicy",
            "generic_metrics": "NoMigrationsPolicy",
        },
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
        assert response.status_code == 403
        assert json.loads(response.data)["error"] == "Group not allowed"

        response = admin_api.get("/migrations/bad_group/list")
        assert response.status_code == 403

    with patch(
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS",
        {
            "sessions": "NonBlockingMigrationsPolicy",
            "generic_metrics": "NoMigrationsPolicy",
        },
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
        "snuba.settings.ADMIN_ALLOWED_MIGRATION_GROUPS",
        {
            "system": "AllMigrationsPolicy",
            "invalid_but_allowed_group": "AllMigrationsPolicy",
            "generic_metrics": "NoMigrationsPolicy",
            "events": "NonBlockingMigrationsPolicy",
        },
    ):
        # invalid action
        response = admin_api.post("/migrations/system/invalid_action/0001_migrations/")
        assert response.status_code == 404

        # invalid migration group
        response = admin_api.post(
            f"/migrations/invalid_but_allowed_group/{action}/0001_migrations"
        )
        assert response.status_code == 500

        with patch.object(Runner, method) as mock_run_migration:
            response = admin_api.post(f"/migrations/system/{action}/0001_migrations")
            assert response.status_code == 200
            migration_key = MigrationKey(
                group=MigrationGroup.SYSTEM, migration_id="0001_migrations"
            )
            mock_run_migration.assert_called_once_with(
                migration_key, force=False, fake=False, dry_run=False
            )

        with patch.object(Runner, method) as mock_run_migration:
            # not allowed migration group
            response = admin_api.post(f"/migrations/sessions/{action}/0001_sessions")
            assert response.status_code == 403
            assert json.loads(response.data) == {"error": "Group not allowed"}
            assert mock_run_migration.call_count == 0

            # not allowed migration group policy
            response = admin_api.post(
                f"/migrations/generic_metrics/{action}/0003_sets_mv"
            )
            assert response.status_code == 403
            assert json.loads(response.data) == {
                "error": f"Group not allowed {action} policy"
            }
            assert mock_run_migration.call_count == 0

            # forced migration
            response = admin_api.post(
                f"/migrations/system/{action}/0001_migrations?force=true"
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

        with patch.object(Runner, method) as mock_run_migration:
            # not allowed non blocking
            response = admin_api.post(
                f"/migrations/events/{action}/0014_backfill_errors"
            )
            assert response.status_code == 403
            assert json.loads(response.data) == {
                "error": f"Group not allowed {action} policy"
            }
            assert mock_run_migration.call_count == 0

            if action == "run":
                # allowed non blocking
                response = admin_api.post(
                    f"/migrations/events/{action}/0015_truncate_events"
                )
                assert response.status_code == 200
                migration_key = MigrationKey(
                    group=MigrationGroup.EVENTS, migration_id="0015_truncate_events"
                )
                mock_run_migration.assert_called_once_with(
                    migration_key, force=False, fake=False, dry_run=False
                )

            if action == "reverse":
                with patch.object(Runner, method) as mock_run_migration:
                    # allowed non blocking
                    with patch(
                        "snuba.migrations.runner.Runner.get_status",
                        return_value=(Status.IN_PROGRESS, None),
                    ):
                        migration_key = MigrationKey(
                            group=MigrationGroup.EVENTS,
                            migration_id="0016_drop_legacy_events",
                        )
                        response = admin_api.post(
                            f"/migrations/events/{action}/0016_drop_legacy_events"
                        )
                        assert response.status_code == 200
                        mock_run_migration.assert_called_once_with(
                            migration_key, force=False, fake=False, dry_run=False
                        )

            # allow dry runs
        with patch.object(Runner, method) as mock_run_migration:

            def print_something(*args: Any, **kwargs: Any) -> None:
                print("a dry run")

            mock_run_migration.side_effect = print_something
            response = admin_api.post(
                f"/migrations/events/{action}/0014_backfill_errors?dry_run=true"
            )
            assert response.status_code == 200
            assert json.loads(response.data) == {"stdout": "a dry run\n"}
            assert mock_run_migration.call_count == 1
