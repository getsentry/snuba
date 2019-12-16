import logging
import os
import time

from datetime import datetime
from flask import Flask, redirect, render_template, request as http_request
from markdown import markdown
from uuid import uuid1
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.gnu_backtrace import GnuBacktraceIntegration
import simplejson as json
from werkzeug.exceptions import BadRequest
import jsonschema
from uuid import UUID

from snuba import schemas, settings, state, util
from snuba.api.query import QueryResult, raw_query
from snuba.api.split import split_query
from snuba.clickhouse.native import ClickhousePool
from snuba.clickhouse.query import DictClickhouseQuery
from snuba.consumer import KafkaMessageMetadata
from snuba.query.schema import SETTINGS_SCHEMA
from snuba.query.timeseries import TimeSeriesExtensionProcessor
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import (
    InvalidDatasetError,
    enforce_table_writer,
    get_dataset,
    get_enabled_dataset_names,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.redis import redis_client
from snuba.util import local_dataset_mode
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.streams.consumer import Payload
from snuba.utils.streams.types import Message, Partition, Topic


logger = logging.getLogger("snuba.api")
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper()), format="%(asctime)s %(message)s"
)

clickhouse_rw = ClickhousePool()
clickhouse_ro = ClickhousePool(client_settings={"readonly": True})


try:
    import uwsgi
except ImportError:

    def check_down_file_exists():
        return False


else:

    def check_down_file_exists():
        try:
            return os.stat("/tmp/snuba.down").st_mtime > uwsgi.started_on
        except OSError:
            return False


def check_clickhouse():
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    """
    try:
        clickhouse_tables = clickhouse_ro.execute("show tables")
        for name in get_enabled_dataset_names():
            dataset = get_dataset(name)
            source = dataset.get_dataset_schemas().get_read_schema()
            if isinstance(source, TableSchema):
                table_name = source.get_table_name()
                if (table_name,) not in clickhouse_tables:
                    return False

        return True

    except Exception:
        return False


application = Flask(__name__, static_url_path="")
application.testing = settings.TESTING
application.debug = settings.DEBUG

sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    integrations=[FlaskIntegration(), GnuBacktraceIntegration()],
    release=os.getenv("SNUBA_RELEASE"),
)


@application.errorhandler(BadRequest)
def handle_bad_request(exception: BadRequest):
    cause = getattr(exception, "__cause__", None)
    if isinstance(cause, json.errors.JSONDecodeError):
        data = {"error": {"type": "json", "message": str(cause)}}
    elif isinstance(cause, jsonschema.ValidationError):
        data = {
            "error": {
                "type": "schema",
                "message": cause.message,
                "path": list(cause.path),
                "schema": cause.schema,
            }
        }
    else:
        data = {"error": {"type": "request", "message": str(exception)}}

    def default_encode(value):
        # XXX: This is necessary for rendering schema defaults values that are
        # generated by callables, rather than constants.
        if callable(value):
            return value()
        else:
            raise TypeError()

    return (
        json.dumps(data, indent=4, default=default_encode),
        400,
        {"Content-Type": "application/json"},
    )


@application.errorhandler(InvalidDatasetError)
def handle_invalid_dataset(exception: InvalidDatasetError):
    data = {"error": {"type": "dataset", "message": str(exception)}}
    return (
        json.dumps(data, sort_keys=True, indent=4),
        404,
        {"Content-Type": "application/json"},
    )


@application.route("/")
def root():
    with open("README.md") as f:
        return render_template("index.html", body=markdown(f.read()))


@application.route("/css/<path:path>")
def send_css(path):
    return application.send_static_file(os.path.join("css", path))


@application.route("/img/<path:path>")
@application.route("/snuba/static/img/<path:path>")
def send_img(path):
    return application.send_static_file(os.path.join("img", path))


@application.route("/dashboard")
@application.route("/dashboard.<fmt>")
def dashboard(fmt="html"):
    if fmt == "json":
        result = {
            "queries": state.get_queries(),
            "concurrent": {k: state.get_concurrent(k) for k in ["global"]},
            "rates": {k: state.get_rates(k) for k in ["global"]},
        }
        return (json.dumps(result), 200, {"Content-Type": "application/json"})
    else:
        return application.send_static_file("dashboard.html")


@application.route("/config")
@application.route("/config.<fmt>", methods=["GET", "POST"])
def config(fmt="html"):
    if fmt == "json":
        if http_request.method == "GET":
            return (
                json.dumps(state.get_raw_configs()),
                200,
                {"Content-Type": "application/json"},
            )
        elif http_request.method == "POST":
            state.set_configs(
                json.loads(http_request.data),
                user=http_request.headers.get("x-forwarded-email"),
            )
            return (
                json.dumps(state.get_raw_configs()),
                200,
                {"Content-Type": "application/json"},
            )
    else:
        return application.send_static_file("config.html")


@application.route("/config/changes.json")
def config_changes():
    return (
        json.dumps(state.get_config_changes()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/health")
def health():
    down_file_exists = check_down_file_exists()
    thorough = http_request.args.get("thorough", False)
    clickhouse_health = check_clickhouse() if thorough else True

    if not down_file_exists and clickhouse_health:
        body = {"status": "ok"}
        status = 200
    else:
        body = {
            "down_file_exists": down_file_exists,
        }
        if thorough:
            body["clickhouse_ok"] = clickhouse_health
        status = 502

    return (json.dumps(body), status, {"Content-Type": "application/json"})


def parse_request_body(http_request):
    with sentry_sdk.start_span(description="parse_request_body", op="parse"):
        try:
            return json.loads(http_request.data)
        except json.errors.JSONDecodeError as error:
            raise BadRequest(str(error)) from error


def validate_request_content(
    body, schema: RequestSchema, timer, dataset: Dataset, referrer: str
) -> Request:
    with sentry_sdk.start_span(
        description="validate_request_content", op="validate"
    ) as span:
        source = dataset.get_dataset_schemas().get_read_schema().get_data_source()
        try:
            request = schema.validate(body, source, referrer)
            span.set_data("snuba_query", request.body)
        except jsonschema.ValidationError as error:
            raise BadRequest(str(error)) from error

        timer.mark("validate_schema")

    return request


@application.route("/query", methods=["GET", "POST"])
@util.time_request("query")
def unqualified_query_view(*, timer: Timer):
    if http_request.method == "GET":
        return redirect(f"/{settings.DEFAULT_DATASET_NAME}/query", code=302)
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        dataset = get_dataset(body.pop("dataset", settings.DEFAULT_DATASET_NAME))
        return dataset_query(dataset, body, timer)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<dataset_name>/query", methods=["GET", "POST"])
@util.time_request("query")
def dataset_query_view(*, dataset_name: str, timer: Timer):
    dataset = get_dataset(dataset_name)
    if http_request.method == "GET":
        schema = RequestSchema.build_with_extensions(dataset.get_extensions())
        return render_template(
            "query.html",
            query_template=json.dumps(schema.generate_template(), indent=4,),
        )
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        return dataset_query(dataset, body, timer)
    else:
        assert False, "unexpected fallthrough"


def dataset_query(dataset, body, timer):
    assert http_request.method == "POST"
    ensure_table_exists(dataset)

    schema = RequestSchema.build_with_extensions(dataset.get_extensions())
    query_result = parse_and_run_query(
        dataset,
        validate_request_content(body, schema, timer, dataset, http_request.referrer),
        timer,
    )

    def json_default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, UUID):
            return str(obj)
        return obj

    return (
        json.dumps(query_result.result, for_json=True, default=json_default),
        query_result.status,
        {"Content-Type": "application/json"},
    )


@split_query
def parse_and_run_query(dataset, request: Request, timer) -> QueryResult:
    from_date, to_date = TimeSeriesExtensionProcessor.get_time_limit(
        request.extensions["timeseries"]
    )

    extensions = dataset.get_extensions()
    for name, extension in extensions.items():
        extension.get_processor().process_query(
            request.query, request.extensions[name], request.settings
        )

    request.query.add_conditions(dataset.default_conditions())

    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in dataset.get_query_processors():
        processor.process_query(request.query, request.settings)

    relational_source = request.query.get_data_source()
    request.query.add_conditions(relational_source.get_mandatory_conditions())

    source = relational_source.format_from()
    with sentry_sdk.start_span(description="create_query", op="db"):
        # TODO: consider moving the performance logic and the pre_where generation into
        # ClickhouseQuery since they are Clickhouse specific
        query = DictClickhouseQuery(dataset, request.query, request.settings)
    timer.mark("prepare_query")

    stats = {
        "clickhouse_table": source,
        "final": request.query.get_final(),
        "referrer": request.referrer,
        "num_days": (to_date - from_date).days,
        "sample": request.query.get_sample(),
    }

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            scope.span.set_tag("dataset", type(dataset).__name__)
            scope.span.set_tag("referrer", http_request.referrer)

    with sentry_sdk.start_span(description=query.format_sql(), op="db") as span:
        span.set_tag("dataset", type(dataset).__name__)
        span.set_tag("table", source)
        return raw_query(request, query, clickhouse_ro, timer, stats)


# Special internal endpoints that compute global aggregate data that we want to
# use internally.


@application.route("/internal/sdk-stats", methods=["POST"])
@util.time_request("sdk-stats")
def sdk_distribution(*, timer: Timer):
    dataset = get_dataset("events")
    request = validate_request_content(
        parse_request_body(http_request),
        RequestSchema(
            schemas.SDK_STATS_BASE_SCHEMA,
            SETTINGS_SCHEMA,
            schemas.SDK_STATS_EXTENSIONS_SCHEMA,
        ),
        timer,
        dataset,
        http_request.referrer,
    )

    request.query.set_aggregations(
        [["uniq", "project_id", "projects"], ["count()", None, "count"]]
    )
    request.query.add_groupby(["sdk_name", "rtime"])
    request.extensions["project"] = {
        "project": [],
    }

    ensure_table_exists(dataset)

    query_result = parse_and_run_query(dataset, request, timer)
    return (
        json.dumps(
            query_result.result,
            for_json=True,
            default=lambda obj: obj.isoformat() if isinstance(obj, datetime) else obj,
        ),
        query_result.status,
        {"Content-Type": "application/json"},
    )


@application.route("/subscriptions", methods=["POST"])
def create_subscription():
    return (
        json.dumps({"subscription_id": uuid1().hex}),
        202,
        {"Content-Type": "application/json"},
    )


@application.route("/subscriptions/<uuid>/renew", methods=["POST"])
def renew_subscription(uuid):
    return "ok", 202, {"Content-Type": "text/plain"}


@application.route("/subscriptions/<uuid>", methods=["DELETE"])
def delete_subscription(uuid):
    return "ok", 202, {"Content-Type": "text/plain"}


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is checked to avoid scary production mishaps.

    _ensured = {}

    def ensure_table_exists(dataset, force=False):
        if not force and _ensured.get(dataset, False):
            return

        assert local_dataset_mode(), "Cannot create table in distributed mode"

        from snuba import migrate

        # We cannot build distributed tables this way. So this only works in local
        # mode.
        for statement in dataset.get_dataset_schemas().get_create_statements():
            clickhouse_rw.execute(statement)

        migrate.run(clickhouse_rw, dataset)

        _ensured[dataset] = True

    @application.route("/tests/<dataset_name>/insert", methods=["POST"])
    def write(dataset_name):
        from snuba.processor import ProcessorAction

        dataset = get_dataset(dataset_name)
        ensure_table_exists(dataset)

        rows = []
        offset_base = int(round(time.time() * 1000))
        for index, message in enumerate(json.loads(http_request.data)):
            offset = offset_base + index
            processed_message = (
                enforce_table_writer(dataset)
                .get_stream_loader()
                .get_processor()
                .process_message(
                    message, KafkaMessageMetadata(offset=offset, partition=0,)
                )
            )
            if processed_message:
                assert processed_message.action is ProcessorAction.INSERT
                rows.extend(processed_message.data)

        enforce_table_writer(dataset).get_writer().write(rows)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/<dataset_name>/eventstream", methods=["POST"])
    def eventstream(dataset_name):
        dataset = get_dataset(dataset_name)
        ensure_table_exists(dataset)
        record = json.loads(http_request.data)

        version = record[0]
        if version != 2:
            raise RuntimeError("Unsupported protocol version: %s" % record)

        message: Message[Payload] = Message(
            Partition(Topic("topic"), 0),
            0,
            Payload(None, http_request.data),
            datetime.now(),
        )

        type_ = record[1]
        metrics = DummyMetricsBackend()
        if type_ == "insert":
            from snuba.consumer import ConsumerWorker

            worker = ConsumerWorker(
                dataset, producer=None, replacements_topic=None, metrics=metrics
            )
        else:
            from snuba.replacer import ReplacerWorker

            worker = ReplacerWorker(clickhouse_rw, dataset, metrics=metrics)

        processed = worker.process_message(message)
        if processed is not None:
            batch = [processed]
            worker.flush_batch(batch)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/<dataset_name>/drop", methods=["POST"])
    def drop(dataset_name):
        dataset = get_dataset(dataset_name)
        for statement in dataset.get_dataset_schemas().get_drop_statements():
            clickhouse_rw.execute(statement)

        ensure_table_exists(dataset, force=True)
        redis_client.flushdb()
        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/error")
    def error():
        1 / 0


else:

    def ensure_table_exists(dataset, force=False):
        pass
