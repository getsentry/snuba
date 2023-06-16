from __future__ import annotations

import tempfile
from dataclasses import asdict
from typing import Any, Mapping, Sequence
from unittest.mock import patch

import pytest
import simplejson as json
from flask.testing import FlaskClient
from structlog.testing import CapturingLogger

from snuba.admin.auth import _set_roles
from snuba.admin.auth_roles import (
    ROLES,
    generate_migration_test_role,
    generate_tool_test_role,
)
from snuba.admin.clickhouse.migration_checks import run_migration_checks_and_policies
from snuba.admin.user import AdminUser
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.policies import MigrationPolicy
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status
from snuba.redis import RedisClientKey, get_redis_client


@pytest.fixture
def admin_api() -> FlaskClient:
    from snuba.admin.views import application

    return application.test_client()


@pytest.mark.clickhouse_db
def test_migration_groups(admin_api: FlaskClient) -> None:
    runner = Runner()
    with patch("snuba.admin.auth.DEFAULT_ROLES", [generate_tool_test_role("all")]):
        response = admin_api.get("/migrations/groups")

    assert response.status_code == 200
    assert json.loads(response.data) == []

    with patch(
        "snuba.admin.auth.DEFAULT_ROLES",
        [
            generate_migration_test_role("system", "all"),
            generate_migration_test_role("generic_metrics", "non_blocking"),
            generate_tool_test_role("all"),
        ],
    ):
        response = admin_api.get("/migrations/groups")

        def get_migration_ids(
            group: MigrationGroup, policy_name: str
        ) -> Sequence[Mapping[str, str | bool]]:

            _, migrations = run_migration_checks_and_policies(
                {group.value: {MigrationPolicy.class_from_name(policy_name)()}}, runner
            )[0]
            return [asdict(m) for m in migrations]

        assert response.status_code == 200
        assert json.loads(response.data) == [
            {
                "group": "system",
                "migration_ids": get_migration_ids(
                    MigrationGroup.SYSTEM, "AllMigrationsPolicy"
                ),
            },
            {
                "group": "generic_metrics",
                "migration_ids": get_migration_ids(
                    MigrationGroup.GENERIC_METRICS, "NonBlockingMigrationsPolicy"
                ),
            },
        ]


@pytest.mark.clickhouse_db
def test_list_migration_status(admin_api: FlaskClient) -> None:
    with patch(
        "snuba.admin.auth.DEFAULT_ROLES",
        [
            generate_migration_test_role("system", "non_blocking"),
            generate_migration_test_role("generic_metrics", "none"),
            generate_tool_test_role("all"),
        ],
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
        "snuba.admin.auth.DEFAULT_ROLES",
        [
            generate_migration_test_role("test_migration", "non_blocking"),
            generate_migration_test_role("generic_metrics", "none"),
            generate_tool_test_role("all"),
        ],
    ):
        response = admin_api.get("/migrations/test_migration/list")
    assert response.status_code == 200
    expected_json = [
        {
            "blocking": False,
            "migration_id": "0001_create_test_table",
            "status": "completed",
        },
        {
            "blocking": False,
            "migration_id": "0002_add_test_col",
            "status": "completed",
        },
    ]

    def sort_by_migration_id(migration: Any) -> Any:
        return migration["migration_id"]

    sorted_response = sorted(json.loads(response.data), key=sort_by_migration_id)
    sorted_expected_json = sorted(expected_json, key=sort_by_migration_id)

    assert sorted_response == sorted_expected_json


@pytest.mark.clickhouse_db
@pytest.mark.parametrize("action", ["run", "reverse"])
def test_run_reverse_migrations(admin_api: FlaskClient, action: str) -> None:

    method = "run_migration" if action == "run" else "reverse_migration"

    with patch(
        "snuba.admin.auth.DEFAULT_ROLES",
        [
            generate_migration_test_role("system", "all"),
            generate_migration_test_role("generic_metrics", "none"),
            generate_migration_test_role("events", "non_blocking", True),
            generate_tool_test_role("all"),
        ],
    ):
        # invalid action
        response = admin_api.post("/migrations/system/invalid_action/0001_migrations/")
        assert response.status_code == 404

        # invalid migration group
        response = admin_api.post(
            f"/migrations/invalid_but_allowed_group/{action}/0001_migrations"
        )
        assert response.status_code == 403

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


@pytest.mark.redis_db
def test_get_iam_roles(caplog: Any) -> None:
    system_role = generate_migration_test_role("system", "all")
    tool_role = generate_tool_test_role("snql-to-sql")
    with patch(
        "snuba.admin.auth.DEFAULT_ROLES",
        [system_role, tool_role],
    ):
        iam_file = tempfile.NamedTemporaryFile()
        iam_file.write(
            json.dumps(
                {
                    "bindings": [
                        {
                            "members": [
                                "group:team-sns@sentry.io",
                                "user:test_user1@sentry.io",
                            ],
                            "role": "roles/NonBlockingMigrationsExecutor",
                        },
                        {
                            "members": [
                                "group:team-sns@sentry.io",
                                "user:test_user1@sentry.io",
                                "user:test_user2@sentry.io",
                            ],
                            "role": "roles/TestMigrationsExecutor",
                        },
                        {
                            "members": [
                                "group:team-sns@sentry.io",
                                "user:test_user1@sentry.io",
                                "user:test_user2@sentry.io",
                            ],
                            "role": "roles/owner",
                        },
                        {
                            "members": [
                                "group:team-sns@sentry.io",
                                "user:test_user1@sentry.io",
                            ],
                            "role": "roles/AllTools",
                        },
                    ]
                }
            ).encode("utf-8")
        )
        iam_file.flush()

        with patch("snuba.admin.auth.settings.ADMIN_IAM_POLICY_FILE", iam_file.name):

            user1 = AdminUser(email="test_user1@sentry.io", id="unknown")
            _set_roles(user1)

            assert user1.roles == [
                ROLES["NonBlockingMigrationsExecutor"],
                ROLES["TestMigrationsExecutor"],
                ROLES["AllTools"],
                system_role,
                tool_role,
            ]

            _set_roles(user1)

            user2 = AdminUser(email="test_user2@sentry.io", id="unknown")
            _set_roles(user2)

            assert user2.roles == [
                ROLES["TestMigrationsExecutor"],
                system_role,
                tool_role,
            ]

            user3 = AdminUser(email="test_user3@sentry.io", id="unknown")
            _set_roles(user3)

            assert user3.roles == [
                system_role,
                tool_role,
            ]

        iam_file.write(json.dumps({"bindings": []}).encode("utf-8"))
        iam_file.flush()

        with patch("snuba.admin.auth.settings.ADMIN_IAM_POLICY_FILE", iam_file.name):
            _set_roles(user1)

            assert user1.roles == [
                ROLES["NonBlockingMigrationsExecutor"],
                ROLES["TestMigrationsExecutor"],
                ROLES["AllTools"],
                system_role,
                tool_role,
            ]

            redis_client = get_redis_client(RedisClientKey.ADMIN_AUTH)
            redis_client.delete(user1.email)
            _set_roles(user1)

            assert user1.roles == [
                system_role,
                tool_role,
            ]

        with patch(
            "snuba.admin.auth.settings.ADMIN_IAM_POLICY_FILE", "file_not_exists.json"
        ):
            log = CapturingLogger()
            with patch("snuba.admin.auth.logger", log):
                user3 = AdminUser(email="test_user3@sentry.io", id="unknown")
                _set_roles(user3)
                assert "IAM policy file not found file_not_exists.json" in str(
                    log.calls
                )
