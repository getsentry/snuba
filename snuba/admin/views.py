from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from dataclasses import asdict
from datetime import datetime
from typing import Any, List, Mapping, Optional, Sequence, Tuple, Type, cast

import sentry_sdk
import simplejson as json
import structlog
from flask import Flask, Response, g, jsonify, make_response, request
from google.protobuf.json_format import MessageToDict, Parse
from structlog.contextvars import bind_contextvars, clear_contextvars

from snuba import settings, state
from snuba.admin.audit_log.action import AuditLogAction
from snuba.admin.audit_log.base import AuditLog
from snuba.admin.auth import USER_HEADER_KEY, UnauthorizedException, authorize_request
from snuba.admin.cardinality_analyzer.cardinality_analyzer import run_metrics_query
from snuba.admin.clickhouse.capacity_management import (
    get_storages_with_allocation_policies,
)
from snuba.admin.clickhouse.common import InvalidCustomQuery, InvalidNodeError
from snuba.admin.clickhouse.copy_tables import copy_tables
from snuba.admin.clickhouse.database_clusters import get_node_info, get_system_settings
from snuba.admin.clickhouse.migration_checks import run_migration_checks_and_policies
from snuba.admin.clickhouse.nodes import get_storage_info
from snuba.admin.clickhouse.predefined_cardinality_analyzer_queries import (
    CardinalityQuery,
)
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
from snuba.admin.dead_letter_queue import get_dlq_topics
from snuba.admin.kafka.topics import get_broker_data
from snuba.admin.migrations_policies import (
    check_migration_perms,
    get_migration_group_policies,
)
from snuba.admin.production_queries.prod_queries import run_mql_query, run_snql_query
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
from snuba.configs.configuration import ConfigurableComponent
from snuba.consumers.dlq import (
    DlqInstruction,
    DlqInstructionStatus,
    DlqReplayPolicy,
    clear_instruction,
    load_instruction,
    store_instruction,
)
from snuba.datasets.factory import InvalidDatasetError, get_enabled_dataset_names
from snuba.datasets.storages.factory import get_storage, get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
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
from snuba.query.query_settings import HTTPQuerySettings
from snuba.replacers.replacements_and_expiry import (
    get_config_auto_replacements_bypass_projects,
)
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.schema import RequestSchema
from snuba.state.explain_meta import explain_cleanup, get_explain_meta
from snuba.utils.metrics.timer import Timer
from snuba.utils.registered_class import InvalidConfigKeyError
from snuba.web.delete_query import (
    DeletesNotEnabledError,
    delete_from_storage,
    deletes_are_enabled,
)
from snuba.web.rpc import RPCEndpoint, list_all_endpoint_names, run_rpc_handler
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
)
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
                "profilesSampleRate": settings.ADMIN_PROFILES_SAMPLE_RATE,
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


@application.route("/cardinality_queries")
@check_tool_perms(tools=[AdminTools.CARDINALITY_ANALYZER])
def cardinality_queries() -> Response:
    res = [q.to_json() for q in CardinalityQuery.all_classes()]
    return make_response(jsonify(res), 200)


@application.route("/kafka")
@check_tool_perms(tools=[AdminTools.KAFKA])
def kafka_topics() -> Response:
    return make_response(jsonify(get_broker_data()), 200)


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
        rows, columns = cast(List[List[str]], result.results), result.meta

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

        resp = copy_tables(
            source_host=source_host,
            storage_name=storage,
            dry_run=dry_run,
            target_host=target_host,
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
    except InvalidDatasetError as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )
    finally:
        explain_cleanup()


@application.route("/routing_strategies")
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def routing_strategies() -> Response:
    return Response(
        json.dumps(BaseRoutingStrategy.all_names()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/storages_with_allocation_policies")
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def storages_with_allocation_policies() -> Response:
    return Response(
        json.dumps(get_storages_with_allocation_policies()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/routing_strategy_configs/<path:strategy_name>", methods=["GET"])
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def get_routing_strategy_configs(strategy_name: str) -> Response:
    strategy = BaseRoutingStrategy.get_from_name(strategy_name)()

    return Response(
        json.dumps(strategy.to_dict()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/allocation_policy_configs/<path:storage_key>", methods=["GET"])
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def get_allocation_policy_configs(storage_key: str) -> Response:
    storage = get_storage(StorageKey(storage_key))
    policies = storage.get_allocation_policies() + storage.get_delete_allocation_policies()

    return Response(
        json.dumps([policy.to_dict() for policy in policies]),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/set_configurable_component_configuration", methods=["POST", "DELETE"])
@check_tool_perms(tools=[AdminTools.CAPACITY_MANAGEMENT])
def set_configuration() -> Response:
    data = json.loads(request.data)
    user = request.headers.get(USER_HEADER_KEY)

    try:
        configurable_component_namespace = data["configurable_component_namespace"]
        configurable_component_class_name = data["configurable_component_class_name"]
        resource_name = data["resource_name"]
        assert isinstance(configurable_component_namespace, str), (
            f"Invalid configurable_component_namespace: {configurable_component_namespace}"
        )
        assert isinstance(configurable_component_class_name, str), (
            f"Invalid configurable_component_class_name: {configurable_component_class_name}"
        )
        assert isinstance(resource_name, str), f"Invalid resource_name {resource_name}"
        configurable_component = (
            ConfigurableComponent.get_component_class(configurable_component_namespace)
            .get_from_name(configurable_component_class_name)
            .create_minimal_instance(resource_name)
        )

        key = data["key"]
        params = data.get("params", {})
        assert isinstance(key, str), "Invalid key"
        assert isinstance(params, dict), "Invalid params"
        assert key != "", "Key cannot be empty string"

    except Exception as exc:
        return Response(
            json.dumps({"error": f"Invalid config: {str(exc)}"}),
            400,
            {"Content-Type": "application/json"},
        )

    if request.method == "DELETE":
        configurable_component.delete_config_value(config_key=key, params=params, user=user)
        audit_log.record(
            user or "",
            AuditLogAction.CONFIGURABLE_COMPONENT_DELETE,
            {
                "resource_identifier": resource_name,
                "configurable_component_class_name": configurable_component.class_name(),
                "key": key,
            },
            notify=True,
        )
        return Response("", 200)
    elif request.method == "POST":
        try:
            value = data["value"]
            assert isinstance(value, str), "Invalid value"
            configurable_component.set_config_value(
                config_key=key, value=value, params=params, user=user
            )
            audit_log.record(
                user or "",
                AuditLogAction.CONFIGURABLE_COMPONENT_UPDATE,
                {
                    "resource_identifier": resource_name,
                    "configurable_component_class_name": configurable_component.class_name(),
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


@application.route("/cardinality_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.CARDINALITY_ANALYZER])
def cardinality_analyzer_query() -> Response:
    # HACK (Volo):
    # mostly copypasta from querylog, should not stick around for too long
    # when production query tool gets made this should not be necessary
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
        result = run_metrics_query(raw_sql, user)
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


@application.route("/dead_letter_queue", methods=["GET"])
@check_tool_perms(tools=[AdminTools.KAFKA])
def dlq_topics() -> Response:
    return make_response(jsonify(get_dlq_topics()), 200)


@application.route("/dead_letter_queue/replay", methods=["GET", "POST", "DELETE"])
@check_tool_perms(tools=[AdminTools.KAFKA])
def dlq_replay() -> Response:
    if request.method == "POST":
        req = request.get_json() or {}  # Required for typing

        try:
            policy = DlqReplayPolicy(req["policy"])
            storage_key = StorageKey(req["storage"])
            slice_id = req["slice"]
            max_messages_to_process = req["maxMessages"]
            assert max_messages_to_process > 0, "maxMessages must be greater than 1"
        except (KeyError, AssertionError):
            return make_response("Missing required fields", 400)

        if load_instruction() is not None:
            return make_response("Instruction exists", 400)

        instruction = DlqInstruction(
            policy,
            DlqInstructionStatus.NOT_STARTED,
            storage_key,
            slice_id,
            max_messages_to_process,
        )

        user = request.headers.get(USER_HEADER_KEY)

        audit_log.record(
            user or "",
            AuditLogAction.DLQ_REPLAY,
            {"instruction": str(instruction)},
            notify=True,
        )
        store_instruction(instruction)

    if request.method == "DELETE":
        clear_instruction()

    loaded_instruction = load_instruction()

    if loaded_instruction is None:
        return make_response(jsonify(None), 200)

    return make_response(loaded_instruction.to_bytes().decode("utf-8"), 200)


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
        endpoint_class: Type[RPCEndpoint[Any, Any]] = RPCEndpoint.get_from_name(
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
    except InvalidDatasetError as exception:
        return Response(
            json.dumps({"error": {"message": str(exception)}}, indent=4),
            400,
            {"Content-Type": "application/json"},
        )


@application.route("/production_mql_query", methods=["POST"])
@check_tool_perms(tools=[AdminTools.PRODUCTION_QUERIES])
def production_mql_query() -> Response:
    body = json.loads(request.data)
    body["tenant_ids"] = {"referrer": request.referrer, "organization_id": ORG_ID}
    try:
        return run_mql_query(body, g.user.email)
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


@application.route("/allowed_projects", methods=["GET"])
@check_tool_perms(tools=[AdminTools.PRODUCTION_QUERIES])
def get_allowed_projects() -> Response:
    return make_response(jsonify(settings.ADMIN_ALLOWED_PROD_PROJECTS), 200)


@application.route("/admin_regions", methods=["GET"])
def get_admin_regions() -> Response:
    return make_response(jsonify(settings.ADMIN_REGIONS), 200)


@application.route(
    "/delete",
    methods=["DELETE"],
)
@check_tool_perms(tools=[AdminTools.DELETE_TOOL])
def delete() -> Response:
    """
    Given a storage name and columns object, parses the input and calls
    delete_from_storage with them.

    Input:
        an http DELETE request with a json body containing elements "storage" and "columns"
        see delete_from_storage for definition of these inputs.
    """
    body = request.get_json()
    assert isinstance(body, dict)
    storage = body.pop("storage", None)
    if storage is None:
        return make_response(
            jsonify(
                {
                    "error",
                    "all required input 'storage' is not present in the request body",
                }
            ),
            400,
        )
    try:
        storage = get_writable_storage(StorageKey(storage))
    except Exception as e:
        return make_response(
            jsonify(
                {
                    "error": str(e),
                }
            ),
            400,
        )
    try:
        schema = RequestSchema.build(HTTPQuerySettings, is_delete=True)
        request_parts = schema.validate(body)
        delete_results = delete_from_storage(
            storage,
            request_parts.query["query"]["columns"],
            request_parts.attribution_info,
        )
    except (InvalidJsonRequestException, DeletesNotEnabledError) as schema_error:
        return make_response(
            jsonify({"error": str(schema_error)}),
            400,
        )
    except Exception as e:
        if application.debug:
            from traceback import format_exception

            return make_response(jsonify({"error": format_exception(e)}), 500)
        else:
            sentry_sdk.capture_exception(e)
            return make_response(jsonify({"error": "unexpected internal error"}), 500)

    return Response(json.dumps(delete_results), 200, {"Content-Type": "application/json"})


@application.route(
    "/deletes-enabled",
    methods=["GET"],
)
@check_tool_perms(tools=[AdminTools.DELETE_TOOL])
def deletes_enabled() -> Response:
    return make_response(jsonify(deletes_are_enabled()), 200)


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


@application.route("/clickhouse_node_info")
@check_tool_perms(tools=[AdminTools.DATABASE_CLUSTERS])
def clickhouse_node_info() -> Response:
    try:
        node_info = get_node_info()
        return make_response(jsonify(node_info), 200)
    except Exception as e:
        return make_response(jsonify({"error": str(e)}), 500)


@application.route("/clickhouse_system_settings")
@check_tool_perms(tools=[AdminTools.DATABASE_CLUSTERS])
def clickhouse_system_settings() -> Response:
    host = request.args.get("host")
    port = request.args.get("port")
    storage = request.args.get("storage")
    if not all([host, port, storage]):
        return make_response(jsonify({"error": "Host, port, and storage are required"}), 400)
    try:
        # conversions for typing
        settings = get_system_settings(str(host), int(str(port)), str(storage))
        return make_response(jsonify(settings), 200)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return make_response(jsonify({"error": str(e)}), 500)
