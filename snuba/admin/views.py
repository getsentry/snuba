from __future__ import annotations

import io
import sys
import uuid
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

import sentry_sdk
import simplejson as json
import structlog
from flask import Flask, Response, g, jsonify, make_response, request
from google.protobuf.json_format import MessageToDict, Parse
from structlog.contextvars import bind_contextvars, clear_contextvars
from werkzeug.exceptions import HTTPException

from snuba import settings, state
from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.auth import USER_HEADER_KEY, UnauthorizedException, authorize_request
from snuba.admin.clickhouse.common import InvalidCustomQuery, InvalidNodeError
from snuba.admin.clickhouse.copy_tables import copy_tables
from snuba.admin.clickhouse.migration_checks import run_migration_checks_and_policies
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.clickhouse.predefined_querylog_queries import QuerylogQuery
from snuba.admin.clickhouse.predefined_system_queries import SystemQuery
from snuba.admin.clickhouse.profile_events import gather_profile_events
from snuba.admin.clickhouse.querylog import describe_querylog_schema, run_querylog_query
from snuba.admin.clickhouse.system_queries import (
    UnauthorizedForSudo,
    run_system_query_on_host_with_sql,
)
from snuba.admin.clickhouse.trace_log_parsing import summarize_trace_output
from snuba.admin.clickhouse.tracing import TraceOutput, run_query_and_get_trace
from snuba.admin.migrations_policies import (
    check_migration_perms,
    get_migration_group_policies,
)
from snuba.admin.production_queries.prod_queries import run_snql_query
from snuba.admin.rpc.rpc_queries import validate_request_meta
from snuba.admin.runtime_config import (
    ConfigChange,
    ConfigType,
    get_config_type_from_value,
)
from snuba.admin.tool_policies import (
    AdminTools,
    check_tool_perms,
    get_user_allowed_tools,
)
from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.factory import InvalidDatasetError, get_enabled_dataset_names
from snuba.manual_jobs import Job, JobSpec
from snuba.manual_jobs.runner import (
    list_job_specs,
    list_job_specs_with_status,
    run_job,
    view_job_logs,
)
from snuba.migrations.errors import InactiveClickhouseReplica, MigrationError
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import MigrationKey, Runner
from snuba.migrations.status import Status
from snuba.query.exceptions import InvalidQueryException
from snuba.replacers.replacements_and_expiry import (
    get_config_auto_replacements_bypass_projects,
)
from snuba.state.explain_meta import explain_cleanup, get_explain_meta
from snuba.utils.metrics.timer import Timer
from snuba.utils.registered_class import InvalidConfigKeyError
from snuba.web.rpc import RPCEndpoint, list_all_endpoint_names, run_rpc_handler
from snuba.web.views import dataset_query

logger = structlog.get_logger().bind(module=__name__)

application = Flask(__name__, static_url_path="/static", static_folder="dist")

runner = Runner()
audit_log = AuditLog()

ORG_ID = 1


@application.errorhandler(UnauthorizedException)
def handle_invalid_json(exception: UnauthorizedException) -> Response:
    return Response(
        json.dumps({"error": "Unauthorized"}),
        401,
        {"Content-Type": "application/json"},
    )


@application.errorhandler(InvalidDatasetError)
def handle_invalid_dataset(exception: InvalidDatasetError) -> Response:
    data = {"error": {"type": "dataset", "message": str(exception)}}
    return Response(
        json.dumps(data, sort_keys=True, indent=4),
        404,
        {"Content-Type": "application/json"},
    )


# passthrough needed so that we don't turn these into 500s
@application.errorhandler(HTTPException)
def handle_http_exception(exception: HTTPException) -> HTTPException:
    return exception


@application.errorhandler(Exception)
def handle_uncaught_exception(exception: Exception) -> Response:
    logger.error(exception, exc_info=True)
    return Response(
        json.dumps({"error": {"type": "unknown", "message": str(exception)}}),
        500,
        {"Content-Type": "application/json"},
    )


@application.before_request
def set_logging_context() -> None:
    clear_contextvars()
    bind_contextvars(endpoint=request.endpoint, user_ip=request.remote_addr)


@application.before_request
def authorize() -> None:
    if request.endpoint != "health":
        user = authorize_request()
        with sentry_sdk.push_scope() as scope:
            scope.user = {"email": user.email}
            g.user = user


@application.after_request
def set_profiling_header(response: Response) -> Response:
    response.headers["Document-Policy"] = "js-profiling"
    return response


@application.route("/")
def root() -> Response:
    return application.send_static_file("index.html")


@application.route("/health")
def health() -> Response:
    return Response("OK", 200)


@application.route("/settings")
def settings_endpoint() -> Response:
    """
    IMPORTANT: This endpoint is only secure because the admin tool is only exposed on
    our internal network. If this ever becomes a public app, this is a security risk.
    """
    # This must mirror the Settings type in the frontend code
    return make_response(
        jsonify(
            {
                "dsn": settings.ADMIN_FRONTEND_DSN,
                "tracesSampleRate": settings.ADMIN_TRACE_SAMPLE_RATE,
                "profileSessionSampleRate": settings.ADMIN_PROFILES_SAMPLE_RATE,
                "tracePropagationTargets": settings.ADMIN_FRONTEND_TRACE_PROPAGATION_TARGETS,
                "replaysSessionSampleRate": settings.ADMIN_REPLAYS_SAMPLE_RATE,
                "replaysOnErrorSampleRate": settings.ADMIN_REPLAYS_SAMPLE_RATE_ON_ERROR,
                "userEmail": g.user.email,
            }
        ),
        200,
    )


@application.route("/tools")
def tools() -> Response:
    return make_response(jsonify({"tools": [t.value for t in get_user_allowed_tools(g.user)]}), 200)


@application.route("/migrations/groups")
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def migrations_groups() -> Response:
    group_policies = get_migration_group_policies(g.user)
    allowed_groups = group_policies.keys()

    res: list[Mapping[str, str | Sequence[Mapping[str, str | bool]]]] = []
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
                        for migration_id, status, blocking, _ in runner_group_migrations
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
    return run_or_reverse_migration(group=group, action="run", migration_id=migration_id)


@application.route(
    "/migrations/<group>/reverse/<migration_id>",
    methods=["POST"],
)
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def reverse_migration(group: str, migration_id: str) -> Response:
    return run_or_reverse_migration(group=group, action="reverse", migration_id=migration_id)


@application.route(
    "/migrations/<group>/overwrite/<migration_id>/status/<new_status>",
    methods=["POST"],
)
@check_tool_perms(tools=[AdminTools.MIGRATIONS])
def force_overwrite_migration_status(group: str, migration_id: str, new_status: str) -> Response:
    try:
        migration_group = MigrationGroup(group)
    except ValueError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": "Group not found"}), 400)

    runner.force_overwrite_status(migration_group, migration_id, Status(new_status))
    user = request.headers.get(USER_HEADER_KEY)

    audit_log.record(
        user or "",
        AuditLogAction.FORCE_MIGRATION_OVERWRITE,
        {"group": group, "migration": migration_id, "new_status": new_status},
        notify=True,
    )

    res = {"status": "OK"}
    return make_response(jsonify(res), 200)


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
                (
                    AuditLogAction.RAN_MIGRATION_STARTED
                    if action == "run"
                    else AuditLogAction.REVERSED_MIGRATION_STARTED
                ),
                {"migration": str(migration_key), "force": force, "fake": fake},
            )

        if action == "run":
            runner.run_migration(migration_key, force=force, fake=fake, dry_run=dry_run)
        else:
            runner.reverse_migration(migration_key, force=force, fake=fake, dry_run=dry_run)

        if not dry_run:
            audit_log.record(
                user or "",
                (
                    AuditLogAction.RAN_MIGRATION_COMPLETED
                    if action == "run"
                    else AuditLogAction.REVERSED_MIGRATION_COMPLETED
                ),
                {"migration": str(migration_key), "force": force, "fake": fake},
                notify=True,
            )

    def notify_error() -> None:
        audit_log.record(
            user or "",
            (
                AuditLogAction.RAN_MIGRATION_FAILED
                if action == "run"
                else AuditLogAction.REVERSED_MIGRATION_FAILED
            ),
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
        return make_response(jsonify({"error": "clickhouse error: " + err.message}), 400)
    except InactiveClickhouseReplica as err:
        notify_error()
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": "inactive replicas error: " + err.message}), 400)


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


@application.route("/auto-replacements-bypass-projects")
@check_tool_perms(tools=[AdminTools.AUTO_REPLACEMENTS_BYPASS_PROJECTS])
def auto_replacements_bypass_projects() -> Response:
    def serialize(project_id: int, expiry: datetime) -> Any:
        return {"projectID": project_id, "expiry": str(expiry)}

    data = [
        serialize(project_id, expiry)
        for [project_id, expiry] in get_config_auto_replacements_bypass_projects(
            datetime.now()
        ).items()
    ]
    return Response(json.dumps(data), 200, {"Content-Type": "application/json"})


# Sample cURL command:
#
# curl -X POST \
#  -d '{"host": "127.0.0.1", "port": 9000, "sql": "select count() from system.parts;", storage: "errors", sudo: false}' \
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
        sudo_mode = req.get("sudo", False)
        clusterless_mode = req.get("clusterless", False)
    except KeyError:
        return make_response(jsonify({"error": "Invalid request"}), 400)

    try:
        result = run_system_query_on_host_with_sql(
            host, port, storage, raw_sql, sudo_mode, clusterless_mode, g.user
        )
        rows = []
        rows, columns = cast(list[list[str]], result.results), result.meta

        if columns is not None:
            res = {}
            res["column_names"] = [name for name, _ in columns]
            res["rows"] = [[str(col) for col in row] for row in rows]

            return make_response(jsonify(res), 200)
    except UnauthorizedForSudo as err:
        return make_response(jsonify({"error": err.message or "Cannot sudo"}), 400)

    except InvalidCustomQuery as err:
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

    except ClickhouseError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": err.message or "Invalid query"}), 400)

    except InvalidNodeError as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": err.message or "Invalid node"}), 400)

    # We should never get here
    return make_response(jsonify({"error": "Something went wrong"}), 400)


@application.route("/run_copy_table_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.SYSTEM_QUERIES])
def copy_table_query() -> Response:
    req = request.get_json() or {}
    try:
        storage = req["storage"]
        source_host = req["source_host"]

        dry_run = req.get("dry_run", True)
        target_host = req.get("target_host")
        skip_on_cluster = req.get("skip_on_cluster", False)
        cluster_name_override = req.get("cluster_name")

        resp = copy_tables(
            source_host=source_host,
            storage_name=storage,
            dry_run=dry_run,
            target_host=target_host,
            skip_on_cluster=skip_on_cluster,
            cluster_name_override=cluster_name_override,
        )
    except KeyError as err:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": f"Invalid request, missing key {err.args[0]}",
                    }
                }
            ),
            400,
        )
    except ValueError as err:
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": f"Target host is invalid: {err.args[0]}",
                    }
                }
            ),
            400,
        )
    except ClickhouseError as err:
        logger.error(err, exc_info=True)
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except InvalidNodeError as err:
        logger.error(err, exc_info=True)
        return make_response(
            jsonify(
                {
                    "error": {
                        "type": "request",
                        "message": err.message or "Invalid node",
                    }
                }
            ),
            400,
        )

    try:
        return make_response(jsonify(resp), 200)
    except Exception as err:
        logger.error(err, exc_info=True)
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}),
            500,
        )


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
    try_gather_profile_events = req.get("gather_profile_events", True)
    try:
        settings = {}
        if try_gather_profile_events:
            settings["log_profile_events"] = 1

        query_trace = run_query_and_get_trace(storage, raw_sql, settings)

        if try_gather_profile_events:
            try:
                gather_profile_events(query_trace, storage)
            except Exception:
                logger.warning(
                    "Error gathering profile events, returning trace anyway",
                    exc_info=True,
                )
        return make_response(jsonify(asdict(query_trace)), 200)
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
        logger.error(err, exc_info=True)
        details = {
            "type": "clickhouse",
            "message": str(err),
            "code": err.code,
        }
        return make_response(jsonify({"error": details}), 400)
    except Exception as err:
        logger.error(err, exc_info=True)
        return make_response(
            jsonify({"error": {"type": "unknown", "message": str(err)}}),
            500,
        )


@application.route("/rpc_summarize_trace_with_profile", methods=["POST"])
@check_tool_perms(tools=[AdminTools.QUERY_TRACING])
def summarize_trace_with_profile() -> Response:
    try:
        req = json.loads(request.data)
        trace_logs = req.get("trace_logs")
        storage = req.get("storage", "default")

        if trace_logs is None:
            return make_response(
                jsonify(
                    {
                        "error": {
                            "type": "validation",
                            "message": "Missing required field: trace_logs",
                        }
                    }
                ),
                400,
            )
        if not isinstance(trace_logs, str):
            return make_response(
                jsonify(
                    {
                        "error": {
                            "type": "validation",
                            "message": "trace_logs must be a string",
                        }
                    }
                ),
                400,
            )
        if not trace_logs.strip():
            return make_response(
                jsonify(
                    {
                        "error": {
                            "type": "validation",
                            "message": "trace_logs cannot be empty",
                        }
                    }
                ),
                400,
            )

        summarized_trace_output = summarize_trace_output(trace_logs)
        trace_output = TraceOutput(
            trace_output=trace_logs,
            summarized_trace_output=summarized_trace_output,
            cols=[],
            num_rows_result=0,
            result=[],
            profile_events_results={},
            profile_events_meta=[],
            profile_events_profile={},
        )
        gather_profile_events(trace_output, storage)
        return make_response(jsonify(asdict(trace_output)), 200)
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
        logger.error(err, exc_info=True)
        return make_response(
            jsonify({"error": {"type": "clickhouse", "message": str(err), "code": err.code}}),
            400,
        )
    except Exception as err:
        logger.error(err, exc_info=True)
        return make_response(jsonify({"error": {"type": "unknown", "message": str(err)}}), 500)


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

    descriptions = state.get_all_config_descriptions()

    raw_configs: Sequence[tuple[str, Any]] = state.get_raw_configs().items()

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
            {"option": config_key, "old": old if old else str(old)},
            notify=True,
        )

        return Response("", 200)

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
    except state.MismatchedTypeException:
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
            "old": old if old else str(old),
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
        user: str | None,
        before: ConfigType | None,
        after: ConfigType | None,
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
    return Response(json.dumps(get_storage_info()), 200, {"Content-Type": "application/json"})


@application.route("/snuba_datasets")
@check_tool_perms(tools=[AdminTools.SNQL_TO_SQL])
def snuba_datasets() -> Response:
    return Response(
        json.dumps(get_enabled_dataset_names(), 200, {"Content-Type": "application/json"})
    )


@application.route("/snuba_debug", methods=["POST"])
@check_tool_perms(tools=[AdminTools.SNQL_TO_SQL, AdminTools.SNUBA_EXPLAIN])
def snuba_debug() -> Response:
    body = json.loads(request.data)
    body["debug"] = True
    body["dry_run"] = True
    dataset_name = body.pop("dataset")
    try:
        response = dataset_query(dataset_name, body, Timer("admin"))
        data = response.get_json()
        assert isinstance(data, dict)

        meta = get_explain_meta()
        if meta:
            data["explain"] = asdict(meta)
        response.data = json.dumps(data)
        return response
    except InvalidQueryException as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )
    finally:
        explain_cleanup()


@application.route("/rpc_endpoints", methods=["GET"])
@check_tool_perms(tools=[AdminTools.RPC_ENDPOINTS])
def list_rpc_endpoints() -> Response:
    return Response(
        json.dumps(list_all_endpoint_names()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/rpc_execute/<endpoint_name>/<version>", methods=["POST"])
@check_tool_perms(tools=[AdminTools.RPC_ENDPOINTS])
def execute_rpc_endpoint(endpoint_name: str, version: str) -> Response:
    try:
        endpoint_class: type[RPCEndpoint[Any, Any]] = RPCEndpoint.get_from_name(
            endpoint_name, version
        )
    except InvalidConfigKeyError:
        return Response(
            json.dumps({"error": f"Unknown endpoint: {endpoint_name} or version: {version}"}),
            404,
            {"Content-Type": "application/json"},
        )

    body = request.json

    try:
        request_proto = Parse(json.dumps(body), endpoint_class.request_class()())
        validate_request_meta(request_proto)
        response = run_rpc_handler(endpoint_name, version, request_proto.SerializeToString())
        return Response(
            json.dumps(MessageToDict(response)),
            200,
            {"Content-Type": "application/json"},
        )
    except Exception as e:
        return Response(
            json.dumps({"error": str(e)}),
            400,
            {"Content-Type": "application/json"},
        )


@application.route("/production_snql_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.PRODUCTION_QUERIES])
def production_snql_query() -> Response:
    body = json.loads(request.data)
    body["tenant_ids"] = {"referrer": request.referrer, "organization_id": ORG_ID}
    try:
        return run_snql_query(body, g.user.email)
    except InvalidQueryException as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )


@application.route("/allowed_projects", methods=["GET"])
@check_tool_perms(tools=[AdminTools.PRODUCTION_QUERIES])
def get_allowed_projects() -> Response:
    return make_response(jsonify(settings.ADMIN_ALLOWED_PROD_PROJECTS), 200)


@application.route("/admin_regions", methods=["GET"])
def get_admin_regions() -> Response:
    return make_response(jsonify(settings.ADMIN_REGIONS), 200)


@application.route("/job-specs", methods=["GET"])
@check_tool_perms(tools=[AdminTools.MANUAL_JOBS])
def get_job_specs() -> Response:
    return make_response(jsonify(list_job_specs_with_status()), 200)


@application.route("/job-specs/<job_id>", methods=["POST"])
@check_tool_perms(tools=[AdminTools.MANUAL_JOBS])
def execute_job(job_id: str) -> Response:
    job_specs = list_job_specs()
    job_status = None
    try:
        job_status = run_job(job_specs[job_id])
    except Exception as e:
        return make_response(
            jsonify(
                {
                    "error": str(e),
                }
            ),
            500,
        )

    return make_response(job_status, 200)


@application.route("/job-specs/<job_id>/logs", methods=["GET"])
@check_tool_perms(tools=[AdminTools.MANUAL_JOBS])
def get_job_logs(job_id: str) -> Response:
    return make_response(jsonify(view_job_logs(job_id)), 200)


@application.route("/job-types", methods=["GET"])
@check_tool_perms(tools=[AdminTools.MANUAL_JOBS])
def get_job_types() -> Response:
    """Registered jobs that may be run ad-hoc without a manifest entry. Only
    jobs that opt in via ``allow_adhoc_run`` (read-only / idempotent ones) are
    listed; destructive jobs stay gated behind an explicit manifest entry."""
    adhoc_job_types = sorted(
        name for name in Job.all_names() if Job.class_from_name(name).allow_adhoc_run
    )
    return make_response(jsonify(adhoc_job_types), 200)


@application.route("/job-types/<job_type>/run", methods=["POST"])
@check_tool_perms(tools=[AdminTools.MANUAL_JOBS])
def run_job_by_type(job_type: str) -> Response:
    """Run an ad-hoc-allowed job by its type without needing a manifest entry.
    A fresh job id is generated on every call, so the same job can be run any
    number of times, each run getting its own status and logs."""
    try:
        job_class = Job.class_from_name(job_type)
    except InvalidConfigKeyError:
        job_class = None
    if job_class is None or not job_class.allow_adhoc_run:
        return make_response(
            jsonify(
                {
                    "error": (
                        f"Job type '{job_type}' cannot be run ad-hoc. "
                        "Add a manifest entry to run it."
                    )
                }
            ),
            403,
        )

    try:
        params: dict[Any, Any] = {}
        if request.data:
            body = json.loads(request.data)
            params = body.get("params") or {}
            assert isinstance(params, dict), "`params` must be an object"
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 400)

    job_id = f"{job_type}_{uuid.uuid4().hex}"
    try:
        job_status = run_job(JobSpec(job_id=job_id, job_type=job_type, params=params or None))
    except Exception as e:
        # The runner records status/logs under job_id before raising, so hand
        # it back to let operators inspect the failed run's logs.
        return make_response(jsonify({"error": str(e), "job_id": job_id}), 500)

    return make_response(jsonify({"job_id": job_id, "status": job_status}), 200)
