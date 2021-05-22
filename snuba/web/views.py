import functools
import logging
import os
import time
from datetime import datetime
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
    Text,
    Tuple,
    Union,
)
from uuid import UUID

import jsonschema
import sentry_sdk
import simplejson as json
from flask import Flask, Request, Response, redirect, render_template
from flask import request as http_request
from markdown import markdown
from streaming_kafka_consumer import Message, Partition, Topic
from streaming_kafka_consumer.backends.kafka import KafkaPayload
from werkzeug import Response as WerkzeugResponse
from werkzeug.exceptions import InternalServerError

from snuba import environment, settings, state, util
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.http import JSONRowEncoder
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import (
    InvalidDatasetError,
    enforce_table_writer,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query
from snuba.redis import redis_client
from snuba.request import Language
from snuba.request.exceptions import InvalidJsonRequestException, JsonDecodeException
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.request.schema import RequestParts, RequestSchema
from snuba.request.validation import build_request, parse_legacy_query, parse_snql_query
from snuba.state.rate_limit import RateLimitExceeded
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import InvalidSubscriptionError, PartitionId
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.util import with_span
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException
from snuba.web.converters import DatasetConverter
from snuba.web.query import parse_and_run_query
from snuba.writer import BatchWriterEncoderWrapper, WriterTableRow

metrics = MetricsWrapper(environment.metrics, "api")

logger = logging.getLogger("snuba.api")

# Flask wants a Dict, not a Mapping
RespTuple = Tuple[Text, int, Dict[Any, Any]]

try:
    import uwsgi
except ImportError:

    def check_down_file_exists() -> bool:
        return False


else:

    def check_down_file_exists() -> bool:
        try:
            m_time = os.stat("/tmp/snuba.down").st_mtime
            start_time: float = uwsgi.started_on
            return m_time > start_time
        except OSError:
            return False


def check_clickhouse() -> bool:
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    """
    try:
        for name in get_enabled_dataset_names():
            dataset = get_dataset(name)
            for entity in dataset.get_all_entities():
                for storage in entity.get_all_storages():
                    clickhouse = storage.get_cluster().get_query_connection(
                        ClickhouseClientSettings.QUERY
                    )
                    clickhouse_tables = clickhouse.execute("show tables")
                    source = storage.get_schema()
                    if isinstance(source, TableSchema):
                        table_name = source.get_table_name()
                        if (table_name,) not in clickhouse_tables:
                            return False

        return True

    except Exception:
        return False


def truncate_dataset(dataset: Dataset) -> None:
    for entity in dataset.get_all_entities():
        for storage in entity.get_all_storages():
            cluster = storage.get_cluster()
            clickhouse = cluster.get_query_connection(ClickhouseClientSettings.MIGRATE)
            database = cluster.get_database()

            schema = storage.get_schema()

            if not isinstance(schema, TableSchema):
                return

            table = schema.get_local_table_name()

            clickhouse.execute(f"TRUNCATE TABLE IF EXISTS {database}.{table}")


application = Flask(__name__, static_url_path="")
application.testing = settings.TESTING
application.debug = settings.DEBUG
application.url_map.converters["dataset"] = DatasetConverter


@application.errorhandler(InvalidJsonRequestException)
def handle_invalid_json(exception: InvalidJsonRequestException) -> Response:
    cause = getattr(exception, "__cause__", None)
    data: Mapping[str, Mapping[str, Union[str, Sequence[str]]]]
    if isinstance(cause, json.JSONDecodeError):
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

    def default_encode(value: Callable[..., str]) -> str:
        # XXX: This is necessary for rendering schema defaults values that are
        # generated by callables, rather than constants.
        if callable(value):
            return value()
        else:
            raise TypeError()

    return Response(
        json.dumps(data, indent=4, default=default_encode),
        400,
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


@application.errorhandler(InvalidQueryException)
def handle_invalid_query(exception: InvalidQueryException) -> Response:
    # TODO: Remove this logging as soon as the query validation code is
    # mature enough that we can trust it.
    logger.warning("Invalid query", exc_info=True)

    # TODO: Add special cases with more structure for specific exceptions
    # if needed.
    return Response(
        json.dumps(
            {"error": {"type": "invalid_query", "message": str(exception)}}, indent=4
        ),
        400,
        {"Content-Type": "application/json"},
    )


@application.errorhandler(InternalServerError)
def handle_internal_server_error(exception: InternalServerError) -> Response:
    original = getattr(exception, "original_exception", None)

    if original is None:
        return Response(
            json.dumps(
                {"error": {"type": "internal_server_error", "message": str(exception)}},
                indent=4,
            ),
            500,
            {"Content-Type": "application/json"},
        )

    return Response(
        json.dumps(
            {"error": {"type": "internal_server_error", "message": str(original)}},
            indent=4,
        ),
        500,
        {"Content-Type": "application/json"},
    )


@application.route("/")
def root() -> str:
    with open("README.md") as f:
        return render_template("index.html", body=markdown(f.read()))


@application.route("/css/<path:path>")
def send_css(path: str) -> Response:
    return application.send_static_file(os.path.join("css", path))


@application.route("/img/<path:path>")
@application.route("/snuba/web/static/img/<path:path>")
def send_img(path: str) -> Response:
    return application.send_static_file(os.path.join("img", path))


@application.route("/dashboard")
@application.route("/dashboard.<fmt>")
def dashboard(fmt: str = "html") -> Union[Response, RespTuple]:
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
def config(fmt: str = "html") -> Union[Response, RespTuple]:
    if fmt == "json":
        if http_request.method == "GET":
            return (
                json.dumps(state.get_raw_configs()),
                200,
                {"Content-Type": "application/json"},
            )
        else:
            assert http_request.method == "POST"
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
def config_changes() -> RespTuple:
    return (
        json.dumps(state.get_config_changes()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/health")
def health() -> Response:
    down_file_exists = check_down_file_exists()
    thorough = http_request.args.get("thorough", False)
    clickhouse_health = check_clickhouse() if thorough else True

    body: Mapping[str, Union[str, bool]]
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

    return Response(json.dumps(body), status, {"Content-Type": "application/json"})


def parse_request_body(http_request: Request) -> MutableMapping[str, Any]:
    with sentry_sdk.start_span(description="parse_request_body", op="parse"):
        metrics.timing("http_request_body_length", len(http_request.data))
        try:
            body = json.loads(http_request.data)
            assert isinstance(body, MutableMapping)
            return body
        except json.JSONDecodeError as error:
            raise JsonDecodeException(str(error)) from error


def _trace_transaction(dataset: Dataset) -> None:
    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            scope.span.set_tag("dataset", get_dataset_name(dataset))
            scope.span.set_tag("referrer", http_request.referrer)

        if scope.transaction:
            scope.transaction = f"{scope.transaction.name}__{get_dataset_name(dataset)}__{http_request.referrer}"


@application.route("/query", methods=["GET", "POST"])
@util.time_request("query")
def unqualified_query_view(*, timer: Timer) -> WerkzeugResponse:
    if http_request.method == "GET":
        return redirect(f"/{settings.DEFAULT_DATASET_NAME}/query", code=302)
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        dataset = get_dataset(body.pop("dataset", settings.DEFAULT_DATASET_NAME))
        _trace_transaction(dataset)
        # Not sure what language to pass into dataset_query here
        return dataset_query(dataset, body, timer, Language.LEGACY)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<dataset:dataset>/query", methods=["GET", "POST"])
@util.time_request("query")
def dataset_query_view(*, dataset: Dataset, timer: Timer) -> Union[Response, str]:
    if http_request.method == "GET":
        schema = RequestSchema.build_with_extensions(
            dataset.get_default_entity().get_extensions(),
            HTTPRequestSettings,
            Language.LEGACY,
        )
        return render_template(
            "query.html",
            query_template=json.dumps(schema.generate_template(), indent=4,),
        )
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        _trace_transaction(dataset)
        return dataset_query(dataset, body, timer, Language.LEGACY)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<dataset:dataset>/snql", methods=["GET", "POST"])
@util.time_request("snql")
def snql_dataset_query_view(*, dataset: Dataset, timer: Timer) -> Union[Response, str]:
    if http_request.method == "GET":
        schema = RequestSchema.build_with_extensions(
            {}, HTTPRequestSettings, Language.SNQL,
        )
        return render_template(
            "query.html",
            query_template=json.dumps(schema.generate_template(), indent=4,),
        )
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        _trace_transaction(dataset)
        return dataset_query(dataset, body, timer, Language.SNQL)
    else:
        assert False, "unexpected fallthrough"


@with_span()
def dataset_query(
    dataset: Dataset, body: MutableMapping[str, Any], timer: Timer, language: Language
) -> Response:
    assert http_request.method == "POST"
    referrer = http_request.referrer or "<unknown>"  # mypy

    if language == Language.SNQL:
        metrics.increment("snql.query.incoming", tags={"referrer": referrer})
        parser: Callable[
            [RequestParts, RequestSettings, Dataset],
            Union[Query, CompositeQuery[Entity]],
        ] = partial(parse_snql_query, [])
    else:
        parser = parse_legacy_query

    with sentry_sdk.start_span(description="build_schema", op="validate"):
        schema = RequestSchema.build_with_extensions(
            dataset.get_default_entity().get_extensions(), HTTPRequestSettings, language
        )

    request = build_request(
        body, parser, HTTPRequestSettings, schema, dataset, timer, referrer
    )

    try:
        result = parse_and_run_query(dataset, request, timer)

        # Some metrics to track the adoption of SnQL
        query_type = "simple"
        if language == Language.SNQL:
            if isinstance(request.query, CompositeQuery):
                if isinstance(request.query.get_from_clause(), JoinClause):
                    query_type = "join"
                else:
                    query_type = "subquery"

            metrics.increment(
                "snql.query.success", tags={"referrer": referrer, "type": query_type}
            )

    except QueryException as exception:
        status = 500
        details: Mapping[str, Any]

        cause = exception.__cause__
        if isinstance(cause, RateLimitExceeded):
            status = 429
            details = {
                "type": "rate-limited",
                "message": "rate limit exceeded",
            }
        elif isinstance(cause, ClickhouseError):
            details = {
                "type": "clickhouse",
                "message": str(cause),
                "code": cause.code,
            }
        elif isinstance(cause, Exception):
            details = {
                "type": "unknown",
                "message": str(cause),
            }
        else:
            raise  # exception should have been chained

        if language == Language.SNQL:
            metrics.increment(
                "snql.query.failed", tags={"referrer": referrer, "status": f"{status}"},
            )

        return Response(
            json.dumps(
                {"error": details, "timing": timer.for_json(), **exception.extra}
            ),
            status,
            {"Content-Type": "application/json"},
        )

    payload: MutableMapping[str, Any] = {**result.result, "timing": timer.for_json()}

    if settings.STATS_IN_RESPONSE or request.settings.get_debug():
        payload.update(result.extra)

    return Response(json.dumps(payload), 200, {"Content-Type": "application/json"})


@application.errorhandler(InvalidSubscriptionError)
def handle_subscription_error(exception: InvalidSubscriptionError) -> Response:
    data = {"error": {"type": "subscription", "message": str(exception)}}
    return Response(
        json.dumps(data, indent=4), 400, {"Content-Type": "application/json"},
    )


@application.route("/<dataset:dataset>/subscriptions", methods=["POST"])
@util.time_request("subscription")
def create_subscription(*, dataset: Dataset, timer: Timer) -> RespTuple:
    subscription = SubscriptionDataCodec().decode(http_request.data)
    # TODO: Check for valid queries with fields that are invalid for subscriptions. For
    # example date fields and aggregates.
    identifier = SubscriptionCreator(dataset).create(subscription, timer)
    return (
        json.dumps({"subscription_id": str(identifier)}),
        202,
        {"Content-Type": "application/json"},
    )


@application.route(
    "/<dataset:dataset>/subscriptions/<int:partition>/<key>", methods=["DELETE"]
)
def delete_subscription(*, dataset: Dataset, partition: int, key: str) -> RespTuple:
    SubscriptionDeleter(dataset, PartitionId(partition)).delete(UUID(key))
    return "ok", 202, {"Content-Type": "text/plain"}


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is checked to avoid scary production mishaps.

    @application.route("/tests/<dataset:dataset>/insert", methods=["POST"])
    def write(*, dataset: Dataset) -> RespTuple:
        from snuba.processor import InsertBatch

        rows: MutableSequence[WriterTableRow] = []
        offset_base = int(round(time.time() * 1000))
        for index, message in enumerate(json.loads(http_request.data)):
            offset = offset_base + index
            processed_message = (
                enforce_table_writer(dataset)
                .get_stream_loader()
                .get_processor()
                .process_message(
                    message,
                    KafkaMessageMetadata(
                        offset=offset, partition=0, timestamp=datetime.utcnow()
                    ),
                )
            )
            if processed_message:
                assert isinstance(processed_message, InsertBatch)
                rows.extend(processed_message.rows)

        BatchWriterEncoderWrapper(
            enforce_table_writer(dataset).get_batch_writer(metrics), JSONRowEncoder(),
        ).write(rows)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/<dataset:dataset>/eventstream", methods=["POST"])
    def eventstream(*, dataset: Dataset) -> RespTuple:
        record = json.loads(http_request.data)

        version = record[0]
        if version != 2:
            raise RuntimeError("Unsupported protocol version: %s" % record)

        message: Message[KafkaPayload] = Message(
            Partition(Topic("topic"), 0),
            0,
            KafkaPayload(None, http_request.data, []),
            datetime.now(),
        )

        type_ = record[1]

        storage = dataset.get_default_entity().get_writable_storage()
        assert storage is not None

        if type_ == "insert":
            from streaming_kafka_consumer.strategy_factory import (
                KafkaConsumerStrategyFactory,
            )

            from snuba.consumers.consumer import build_batch_writer, process_message
            from snuba.utils.streams.metrics_adapter import StreamMetricsAdapter

            table_writer = storage.get_table_writer()
            stream_loader = table_writer.get_stream_loader()
            strategy = KafkaConsumerStrategyFactory(
                stream_loader.get_pre_filter(),
                functools.partial(process_message, stream_loader.get_processor()),
                build_batch_writer(table_writer, metrics=metrics),
                max_batch_size=1,
                max_batch_time=1.0,
                processes=None,
                input_block_size=None,
                output_block_size=None,
                metrics=StreamMetricsAdapter(metrics),
            ).create(lambda offsets: None)
            strategy.submit(message)
            strategy.close()
            strategy.join()
        else:
            from snuba.replacer import ReplacerWorker

            worker = ReplacerWorker(storage, metrics=metrics)
            processed = worker.process_message(message)
            if processed is not None:
                batch = [processed]
                worker.flush_batch(batch)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/<dataset:dataset>/drop", methods=["POST"])
    def drop(*, dataset: Dataset) -> RespTuple:
        truncate_dataset(dataset)
        redis_client.flushdb()

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/error")
    def error() -> RespTuple:
        1 / 0
        # unreachable. A valid response is added for mypy
        return ("error", 500, {"Content-Type": "text/plain"})
