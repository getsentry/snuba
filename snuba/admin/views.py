from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from dataclasses import asdict
from typing import Any, List, Mapping, Optional, Sequence, Tuple, cast

import simplejson as json
import structlog
from flask import Flask, Response, g, jsonify, make_response, request
from structlog.contextvars import bind_contextvars, clear_contextvars

from snuba import settings, state
from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.auth import USER_HEADER_KEY, UnauthorizedException, authorize_request
from snuba.admin.clickhouse.capacity_management import (
    get_allocation_policies,
    get_storages_with_allocation_policies,
)
from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.migration_checks import run_migration_checks_and_policies
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.clickhouse.predefined_querylog_queries import QuerylogQuery
from snuba.admin.clickhouse.predefined_system_queries import SystemQuery
from snuba.admin.clickhouse.querylog import describe_querylog_schema, run_querylog_query
from snuba.admin.clickhouse.system_queries import run_system_query_on_host_with_sql
from snuba.admin.clickhouse.tracing import run_query_and_get_trace
from snuba.admin.kafka.topics import get_broker_data
from snuba.admin.migrations_policies import (
    check_migration_perms,
    get_migration_group_policies,
)
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)
from snuba.admin.tool_policies import (
    DEVELOPER_TOOLS,
    AdminTools,
    check_tool_perms,
    get_user_allowed_tools,
)
from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_enabled_dataset_names,
)
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.migrations.connect import check_for_inactive_replicas
from snuba.migrations.errors import InactiveClickhouseReplica, MigrationError
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationKey, Runner
from snuba.query.exceptions import InvalidQueryException
from snuba.utils.metrics.timer import Timer
from snuba.web.views import dataset_query

logger = structlog.get_logger().bind(module=__name__)

application = Flask(__name__, static_url_path="/static", static_folder="dist")

runner = Runner()
audit_log = AuditLog()


@application.errorhandler(UnauthorizedException)
def handle_invalid_json(exception: UnauthorizedException) -> Response:
    return Response(
        json.dumps({"error": "Unauthorized"}),
        401,
        {"Content-Type": "application/json"},
    )


@application.before_request
def set_logging_context() -> None:
    clear_contextvars()
    bind_contextvars(endpoint=request.endpoint, user_ip=request.remote_addr)


@application.before_request
def authorize() -> None:
    logger.debug("authorize.entered")
    if request.endpoint != "health":
        user = authorize_request()
        logger.info("authorize.finished", user=user)
        g.user = user


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


@application.route("/tools")
def tools() -> Response:
    if settings.ADMIN_DEVELOPER_MODE:
        return make_response(
            jsonify({"tools": [t.value for t in DEVELOPER_TOOLS]}), 200
        )

    # TODO: This can return all the tools once developer mode is deployed
    return make_response(
        jsonify({"tools": [t.value for t in get_user_allowed_tools(g.user)]}), 200
    )


@application.route("/migrations/groups")
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def migrations_groups() -> Response:
    group_policies = get_migration_group_policies(g.user)
    allowed_groups = group_policies.keys()

    res: List[Mapping[str, str | Sequence[Mapping[str, str | bool]]]] = []
    if not allowed_groups:
        return make_response(jsonify(res), 200)

    for group, migrations in run_migration_checks_and_policies(group_policies, runner):
        migration_ids = [asdict(m) for m in migrations]
        res.append({"group": group.value, "migration_ids": migration_ids})

    return make_response(jsonify(res), 200)


@application.route("/migrations/<group>/list")
@check_migration_perms
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def migrations_groups_list(group: str) -> Response:
    for runner_group, runner_group_migrations in runner.show_all():
        if runner_group == MigrationGroup(group):
            return make_response(
                jsonify(
                    [
                        {
                            "migration_id": migration_id,
                            "status": status.value,
                            "blocking": blocking,
                        }
                        for migration_id, status, blocking in runner_group_migrations
                    ]
                ),
                200,
            )
    return make_response(jsonify({"error": "Invalid group"}), 400)


@application.route(
    "/migrations/<group>/run/<migration_id>",
    methods=["POST"],
)
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def run_migration(group: str, migration_id: str) -> Response:
    return run_or_reverse_migration(
        group=group, action="run", migration_id=migration_id
    )


@application.route(
    "/migrations/<group>/reverse/<migration_id>",
    methods=["POST"],
)
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def reverse_migration(group: str, migration_id: str) -> Response:
    return run_or_reverse_migration(
        group=group, action="reverse", migration_id=migration_id
    )


@check_migration_perms
def run_or_reverse_migration(group: str, action: str, migration_id: str) -> Response:
    try:
        migration_group = MigrationGroup(group)
    except ValueError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": "Group not found"}), 400)

    migration_key = MigrationKey(migration_group, migration_id)

    def str_to_bool(s: str) -> bool:
        return s.strip().lower() in ("yes", "true", "t", "1")

    force = request.args.get("force", False, type=str_to_bool)
    fake = request.args.get("fake", False, type=str_to_bool)
    dry_run = request.args.get("dry_run", False, type=str_to_bool)

    user = request.headers.get(USER_HEADER_KEY)

    def do_action() -> None:
        if not dry_run:
            audit_log.record(
                user or "",
                AuditLogAction.RAN_MIGRATION_STARTED
                if action == "run"
                else AuditLogAction.REVERSED_MIGRATION_STARTED,
                {"migration": str(migration_key), "force": force, "fake": fake},
            )
            check_for_inactive_replicas()

        if action == "run":
            runner.run_migration(migration_key, force=force, fake=fake, dry_run=dry_run)
        else:
            runner.reverse_migration(
                migration_key, force=force, fake=fake, dry_run=dry_run
            )

        if not dry_run:
            audit_log.record(
                user or "",
                AuditLogAction.RAN_MIGRATION_COMPLETED
                if action == "run"
                else AuditLogAction.REVERSED_MIGRATION_COMPLETED,
                {"migration": str(migration_key), "force": force, "fake": fake},
                notify=True,
            )

    def notify_error() -> None:
        audit_log.record(
            user or "",
            AuditLogAction.RAN_MIGRATION_FAILED
            if action == "run"
            else AuditLogAction.REVERSED_MIGRATION_FAILED,
            {"migration": str(migration_key), "force": force, "fake": fake},
            notify=True,
        )

    try:
        # temporarily redirect stdout to a buffer so we can return it
        with io.StringIO() as output:
            # write output to both the buffer and stdout
            class OutputRedirector(io.StringIO):
                stdout = sys.stdout

                def write(self, s: str) -> int:
                    self.stdout.write(s)
                    return output.write(s)

            with redirect_stdout(OutputRedirector()):
                do_action()
            return make_response(jsonify({"stdout": output.getvalue()}), 200)

    except KeyError as err:
        notify_error()
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": "Group not found"}), 400)
    except MigrationError as err:
        notify_error()
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": "migration error: " + err.message}), 400)
    except ClickhouseError as err:
        notify_error()
        logger.error(err, exc_info=True)
        return make_response(
            jsonify({"error": "clickhouse error: " + err.message}), 400
        )
    except InactiveClickhouseReplica as err:
        notify_error()
        logger.error(err, exc_info=True)
        return make_response(
            jsonify({"error": "inactive replicas error: " + err.message}), 400
        )


@application.route("/clickhouse_queries")
@check_tool_perms(tools=[AdminTools.SYSTEM_QUERIES])
def clickhouse_queries() -> Response:
    res = [q.to_json() for q in SystemQuery.all_classes()]
    return make_response(jsonify(res), 200)


@application.route("/querylog_queries")
@check_tool_perms(tools=[AdminTools.QUERYLOG])
def querylog_queries() -> Response:
    res = [q.to_json() for q in QuerylogQuery.all_classes()]
    return make_response(jsonify(res), 200)


@application.route("/kafka")
@check_tool_perms(tools=[AdminTools.KAFKA])
def kafka_topics() -> Response:
    return make_response(jsonify(get_broker_data()), 200)


# Sample cURL command:
#
# curl -X POST \
#  -d '{"host": "127.0.0.1", "port": 9000, "sql": "select count() from system.parts;", storage: "errors"}' \
#  -H 'Content-Type: application/json' \
#  http://127.0.0.1:1219/run_clickhouse_system_query
@application.route("/run_clickhouse_system_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.SYSTEM_QUERIES])
def clickhouse_system_query() -> Response:
    req = request.get_json()
    try:
        if req is None:  # required for typing
            req = {}

        host = req["host"]
        port = req["port"]
        storage = req["storage"]
        raw_sql = req["sql"]
    except KeyError:
        return make_response(jsonify({"error": "Invalid request"}), 400)

    try:
        result = run_system_query_on_host_with_sql(host, port, storage, raw_sql)
        rows = []
        rows, columns = cast(List[List[str]], result.results), result.meta

        if columns:
            res = {}
            res["column_names"] = [name for name, _ in columns]
            res["rows"] = [[str(col) for col in row] for row in rows]

            return make_response(jsonify(res), 200)
    except InvalidCustomQuery as err:
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

    except ClickhouseError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

    # We should never get here
    return make_response(jsonify({"error": "Something went wrong"}), 400)


# Sample cURL command:
#
# curl -X POST \
#  -H 'Content-Type: application/json' \
#  http://127.0.0.1:1219/clickhouse_trace_query?query=SELECT+count()+FROM+errors_local
@application.route("/clickhouse_trace_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.QUERY_TRACING])
def clickhouse_trace_query() -> Response:
    req = json.loads(request.data)
    try:
        storage = req["storage"]
        raw_sql = req["sql"]
    except KeyError as e:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": f"Invalid request, missing key {e.args[0]}",
                    }
                }
            ),
            400,
        )

    try:
        result = run_query_and_get_trace(storage, raw_sql)
        return make_response(jsonify(asdict(result)), 200)
    except InvalidCustomQuery as err:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "validation",
                        "message": err.message or "Invalid query",
                    }
                }
            ),
            400,
        )
    except ClickhouseError as err:
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except Exception as err:
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}),
            500,
        )


@application.route("/clickhouse_querylog_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.QUERYLOG])
def clickhouse_querylog_query() -> Response:
    user = request.headers.get(USER_HEADER_KEY, "unknown")
    if user == "unknown" and settings.ADMIN_AUTH_PROVIDER != "NOOP":
        return Response(
            json.dumps({"error": "Unauthorized"}),
            401,
            {"Content-Type": "application/json"},
        )
    req = json.loads(request.data)
    try:
        raw_sql = req["sql"]
    except KeyError as e:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": f"Invalid request, missing key {e.args[0]}",
                    }
                }
            ),
            400,
        )
    try:
        result = run_querylog_query(raw_sql, user)
        rows, columns = result.results, result.meta
        if columns:
            return make_response(
                jsonify({"column_names": [name for name, _ in columns], "rows": rows}),
                200,
            )
        return make_response(
            jsonify({"error": {"type": "unknown", "message": "no columns"}}),
            500,
        )
    except ClickhouseError as err:
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except InvalidCustomQuery as err:
        return Response(
            json.dumps({"error": {"message": str(err)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )
    except Exception as err:
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}),
            500,
        )


@application.route("/clickhouse_querylog_schema", methods=["GET"])
@check_tool_perms(tools=[AdminTools.QUERYLOG])
def clickhouse_querylog_schema() -> Response:
    try:
        result = describe_querylog_schema()
        rows, columns = result.results, result.meta
        if columns:
            return make_response(
                jsonify({"column_names": [name for name, _ in columns], "rows": rows}),
                200,
            )
        return make_response(
            jsonify({"error": {"type": "unknown", "message": "no columns"}}),
            500,
        )
    except ClickhouseError as err:
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except InvalidCustomQuery as err:
        return Response(
            json.dumps({"error": {"message": str(err)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )
    except Exception as err:
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}),
            500,
        )


@application.route("/configs", methods=["GET", "POST"])
@check_tool_perms(tools=[AdminTools.CONFIGURATION])
def configs() -> Response:
    if request.method == "POST":
        data = json.loads(request.data)
        try:
            key, value, desc = data["key"], data["value"], data["description"]

            assert isinstance(key, str), "Invalid key"
            assert isinstance(value, str), "Invalid value"
            assert key != "", "Key cannot be empty string"

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )

        existing_config = state.get_uncached_config(key)
        if existing_config is not None:
            return Response(
                json.dumps({"error": f"Config with key {key} exists"}),
                400,
                {"Content-Type": "application/json"},
            )

        user = request.headers.get(USER_HEADER_KEY)

        state.set_config(key, value, user=user)
        state.set_config_description(key, desc, user=user)

        evaluated_value = state.get_uncached_config(key)
        assert evaluated_value is not None
        evaluated_type = get_config_type_from_value(evaluated_value)

        config = {
            "key": key,
            "value": str(evaluated_value),
            "description": state.get_config_description(key),
            "type": evaluated_type,
        }

        audit_log.record(
            user or "",
            AuditLogAction.ADDED_OPTION,
            {"option": key, "new": evaluated_value},
            notify=True,
        )

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})

    else:
        descriptions = state.get_all_config_descriptions()

        raw_configs: Sequence[Tuple[str, Any]] = state.get_raw_configs().items()

        sorted_configs = sorted(raw_configs, key=lambda c: c[0])

        config_data = [
            {
                "key": k,
                "value": str(v) if v is not None else None,
                "description": str(descriptions.get(k)) if k in descriptions else None,
                "type": get_config_type_from_value(v),
            }
            for (k, v) in sorted_configs
        ]

        return Response(
            json.dumps(config_data),
            200,
            {"Content-Type": "application/json"},
        )


@application.route("/all_config_descriptions", methods=["GET"])
@check_tool_perms(tools=[AdminTools.CONFIGURATION])
def all_config_descriptions() -> Response:
    return Response(
        json.dumps(state.get_all_config_descriptions()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/configs/<path:config_key>", methods=["PUT", "DELETE"])
@check_tool_perms(tools=[AdminTools.CONFIGURATION])
def config(config_key: str) -> Response:
    if request.method == "DELETE":
        user = request.headers.get(USER_HEADER_KEY)

        # Get the old value for notifications
        old = state.get_uncached_config(config_key)

        state.delete_config(config_key, user=user)

        if request.args.get("keepDescription") is None:
            state.delete_config_description(config_key, user=user)

        audit_log.record(
            user or "",
            AuditLogAction.REMOVED_OPTION,
            {"option": config_key, "old": str(old) if not old else old},
            notify=True,
        )

        return Response("", 200)

    else:
        # PUT currently only supports editing existing config when old and
        # new types match. Does not currently support passing force to
        # set_config to override the type check.

        user = request.headers.get(USER_HEADER_KEY)
        data = json.loads(request.data)

        # Get the previous value for notifications
        old = state.get_uncached_config(config_key)

        try:
            new_value = data["value"]
            new_desc = data["description"]

            assert isinstance(config_key, str), "Invalid key"
            assert isinstance(new_value, str), "Invalid value"
            assert config_key != "", "Key cannot be empty string"

            state.set_config(
                config_key,
                new_value,
                user=user,
            )
            state.set_config_description(config_key, new_desc, user=user)

        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )
        except (state.MismatchedTypeException):
            return Response(
                json.dumps({"error": "Mismatched type"}),
                400,
                {"Content-Type": "application/json"},
            )

        # Value was updated successfully, refetch and return it
        evaluated_value = state.get_uncached_config(config_key)
        assert evaluated_value is not None
        evaluated_type = get_config_type_from_value(evaluated_value)

        # Send notification
        audit_log.record(
            user or "",
            AuditLogAction.UPDATED_OPTION,
            {
                "option": config_key,
                "old": str(old) if not old else old,
                "new": evaluated_value,
            },
            notify=True,
        )

        config = {
            "key": config_key,
            "value": str(evaluated_value),
            "description": state.get_config_description(config_key),
            "type": evaluated_type,
        }

        return Response(json.dumps(config), 200, {"Content-Type": "application/json"})


@application.route("/config_auditlog")
@check_tool_perms(tools=[AdminTools.AUDIT_LOG])
def config_changes() -> Response:
    def serialize(
        key: str,
        ts: float,
        user: Optional[str],
        before: Optional[ConfigType],
        after: Optional[ConfigType],
    ) -> ConfigChange:
        return {
            "key": key,
            "timestamp": ts,
            "user": user,
            "before": str(before) if before is not None else None,
            "beforeType": get_config_type_from_value(before),
            "after": str(after) if after is not None else None,
            "afterType": get_config_type_from_value(after),
        }

    data = [
        serialize(key, ts, user, before, after)
        for [key, ts, user, before, after] in state.get_config_changes()
    ]

    return Response(json.dumps(data), 200, {"Content-Type": "application/json"})


@application.route("/clickhouse_nodes")
@check_tool_perms(tools=[AdminTools.SYSTEM_QUERIES, AdminTools.QUERY_TRACING])
def clickhouse_nodes() -> Response:
    return Response(
        json.dumps(get_storage_info()), 200, {"Content-Type": "application/json"}
    )


@application.route("/snuba_datasets")
@check_tool_perms(tools=[AdminTools.SNQL_TO_SQL])
def snuba_datasets() -> Response:
    return Response(
        json.dumps(
            get_enabled_dataset_names(), 200, {"Content-Type": "application/json"}
        )
    )


@application.route("/snql_to_sql", methods=["POST"])
@check_tool_perms(tools=[AdminTools.SNQL_TO_SQL])
def snql_to_sql() -> Response:
    body = json.loads(request.data)
    body["debug"] = True
    body["dry_run"] = True
    try:
        dataset = get_dataset(body.pop("dataset"))
        return dataset_query(dataset, body, Timer("admin"))
    except InvalidQueryException as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )
    except InvalidDatasetError as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )


@application.route("/allocation_policies")
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def allocation_policies() -> Response:
    return Response(
        json.dumps(get_allocation_policies()),
        200,
        {"Content-Type": "application/json"},
    )


MULTIPLE_POLICIES = "multiple_allocation_policies_enabled"


@application.route("/storages_with_allocation_policies")
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def storages_with_allocation_policies() -> Response:
    return Response(
        json.dumps(get_storages_with_allocation_policies()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/allocation_policy_configs/<path:storage_key>", methods=["GET"])
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def get_allocation_policy_configs(storage_key: str) -> Response:

    storage = get_storage(StorageKey(storage_key))

    if state.get_config(MULTIPLE_POLICIES, False):
        policies = storage.get_allocation_policies()
        data = [
            {
                "policy_name": policy.config_key(),
                "configs": policy.get_current_configs(),
                "optional_config_definitions": policy.get_optional_config_definitions_json(),
            }
            for policy in policies
        ]
        return Response(json.dumps(data), 200, {"Content-Type": "application/json"})

    policy = storage.get_allocation_policy()
    configs = policy.get_current_configs()
    return Response(json.dumps(configs), 200, {"Content-Type": "application/json"})


@application.route(
    "/allocation_policy_optional_config_definitions/<path:storage>",
    methods=["GET"],
)
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def get_allocation_policy_optional_config_definitions(storage: str) -> Response:
    policy = get_storage(StorageKey(storage)).get_allocation_policy()
    config_definitions = policy.get_optional_config_definitions_json()
    return Response(
        json.dumps(config_definitions), 200, {"Content-Type": "application/json"}
    )


@application.route("/allocation_policy_config", methods=["POST", "DELETE"])
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def set_allocation_policy_config() -> Response:
    data = json.loads(request.data)
    user = request.headers.get(USER_HEADER_KEY)

    try:
        storage, key = (data["storage"], data["key"])

        params = data.get("params", {})

        assert isinstance(storage, str), "Invalid storage"
        assert isinstance(key, str), "Invalid key"
        assert isinstance(params, dict), "Invalid params"
        assert key != "", "Key cannot be empty string"

        if state.get_config(MULTIPLE_POLICIES, False):
            policy_name = data["policy"]
            assert isinstance(policy_name, str), "Invalid policy name"
            policies = get_storage(StorageKey(storage)).get_allocation_policies()
            policy = next(
                (p for p in policies if p.config_key() == policy_name),
                None,
            )
            assert policy is not None, "Policy not found on storage"
        else:
            policy = get_storage(StorageKey(storage)).get_allocation_policy()

    except (KeyError, AssertionError) as exc:
        return Response(
            json.dumps({"error": f"Invalid config: {str(exc)}"}),
            400,
            {"Content-Type": "application/json"},
        )

    if request.method == "DELETE":
        policy.delete_config_value(config_key=key, params=params, user=user)
        audit_log.record(
            user or "",
            AuditLogAction.ALLOCATION_POLICY_DELETE,
            {"storage": storage, "policy": policy.config_key(), "key": key},
            notify=True,
        )
        return Response("", 200)
    elif request.method == "POST":
        try:
            value = data["value"]
            assert isinstance(value, str), "Invalid value"
            policy.set_config_value(
                config_key=key, value=value, params=params, user=user
            )
            audit_log.record(
                user or "",
                AuditLogAction.ALLOCATION_POLICY_UPDATE,
                {
                    "storage": storage,
                    "policy": policy.config_key(),
                    "key": key,
                    "value": value,
                    "params": str(params),
                },
                notify=True,
            )
            return Response("", 200)
        except (KeyError, AssertionError) as exc:
            return Response(
                json.dumps({"error": f"Invalid config: {str(exc)}"}),
                400,
                {"Content-Type": "application/json"},
            )
    else:
        return Response(
            json.dumps({"error": "Method not allowed"}),
            405,
            {"Content-Type": "application/json"},
        )
