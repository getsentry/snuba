from __future__ import annotations

import functools
import itertools
import logging
import os
import random
import time
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Text,
    Tuple,
    Union,
    cast,
)
from uuid import UUID

import jsonschema
import sentry_sdk
import simplejson as json
from arroyo import Message, Partition, Topic
from arroyo.backends.kafka import KafkaPayload
from flask import Flask, Request, Response, redirect, render_template
from flask import request as http_request
from markdown import markdown
from werkzeug import Response as WerkzeugResponse
from werkzeug.exceptions import InternalServerError

from snuba import environment, settings, state, util
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.http import JSONRowEncoder
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ConnectionId,
    UndefinedClickhouseCluster,
)
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity_name
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.entity_subscription import (
    InvalidSubscriptionError,
)
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.query.exceptions import InvalidQueryException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.redis import all_redis_clients
from snuba.request.exceptions import InvalidJsonRequestException, JsonDecodeException
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.state import MismatchedTypeException
from snuba.state.rate_limit import RateLimitExceeded
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.util import with_span
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryTooLongException
from snuba.web.constants import get_http_status_for_clickhouse_error
from snuba.web.converters import DatasetConverter, EntityConverter
from snuba.web.query import parse_and_run_query
from snuba.writer import BatchWriterEncoderWrapper, WriterTableRow

metrics = MetricsWrapper(environment.metrics, "api")

logger = logging.getLogger("snuba.api")

# Flask wants a Dict, not a Mapping
RespTuple = Tuple[Text, int, Dict[Any, Any]]


def shutdown_time() -> Optional[float]:
    try:
        return os.stat("/tmp/snuba.down").st_mtime
    except OSError:
        return None


try:
    IS_SHUTTING_DOWN = False
    import uwsgi
except ImportError:

    def check_down_file_exists() -> bool:
        return False

else:

    def check_down_file_exists() -> bool:
        global IS_SHUTTING_DOWN
        try:
            m_time = shutdown_time()
            if m_time is None:
                return False

            start_time: float = uwsgi.started_on
            IS_SHUTTING_DOWN = m_time > start_time
            return IS_SHUTTING_DOWN
        except OSError:
            return False


def check_clickhouse(
    ignore_experimental: bool = True, metric_tags: dict[str, Any] | None = None
) -> bool:
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    """
    try:
        if ignore_experimental:
            datasets = [
                get_dataset(name)
                for name in get_enabled_dataset_names()
                if not get_dataset(name).is_experimental()
            ]
        else:
            datasets = [get_dataset(name) for name in get_enabled_dataset_names()]

        entities = itertools.chain(
            *[dataset.get_all_entities() for dataset in datasets]
        )
        storages = list(
            itertools.chain(*[entity.get_all_storages() for entity in entities])
        )

        connection_grouped_table_names: MutableMapping[
            ConnectionId, Set[str]
        ] = defaultdict(set)
        for storage in storages:
            if isinstance(storage.get_schema(), TableSchema):
                cluster = storage.get_cluster()
                connection_grouped_table_names[cluster.get_connection_id()].add(
                    cast(TableSchema, storage.get_schema()).get_table_name()
                )
        # De-dupe clusters by host:TCP port:HTTP port:database
        unique_clusters = {
            storage.get_cluster().get_connection_id(): storage.get_cluster()
            for storage in storages
        }

        for (cluster_key, cluster) in unique_clusters.items():
            clickhouse = cluster.get_query_connection(ClickhouseClientSettings.QUERY)
            clickhouse_tables = clickhouse.execute("show tables").results
            known_table_names = connection_grouped_table_names[cluster_key]
            logger.debug(f"checking for {known_table_names} on {cluster_key}")
            for table in known_table_names:
                if (table,) not in clickhouse_tables:
                    logger.error(f"{table} not present in cluster {cluster}")
                    if metric_tags is not None:
                        metric_tags["table_not_present"] = table
                        metric_tags["cluster"] = str(cluster)
                    return False

        return True

    except UndefinedClickhouseCluster as err:
        if metric_tags is not None and isinstance(err.extra_data, dict):
            metric_tags.update(err.extra_data)
        logger.error(err)
        return False

    except ClickhouseError as err:
        if metric_tags and err.__cause__:
            metric_tags["exception"] = type(err.__cause__).__name__
        logger.error(err)
        return False

    except Exception as err:
        logger.error(err)
        return False


def truncate_dataset(dataset: Dataset) -> None:
    for entity in dataset.get_all_entities():
        for storage in entity.get_all_storages():
            cluster = storage.get_cluster()
            nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
            for node in nodes:
                clickhouse = cluster.get_node_connection(
                    ClickhouseClientSettings.MIGRATE, node
                )

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
application.url_map.converters["entity"] = EntityConverter


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
    if exception.should_report:
        logger.warning("Invalid query", exc_info=exception)
    else:
        logger.info("Invalid query", exc_info=exception)

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
    with open("README.rst") as f:
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
            try:
                state.set_configs(
                    json.loads(http_request.data),
                    user=http_request.headers.get("x-forwarded-email"),
                )
                return (
                    json.dumps(state.get_raw_configs()),
                    200,
                    {"Content-Type": "application/json"},
                )
            except MismatchedTypeException as exc:
                return (
                    json.dumps(
                        {
                            "error": {
                                "type": "client_error",
                                "message": "Existing value and New value have different types. Use option force to override check",
                                "key": str(exc.key),
                                "original value type": str(exc.original_type),
                                "new_value_type": str(exc.new_type),
                            }
                        },
                        indent=4,
                    ),
                    400,
                    {"Content-Type": "application/json"},
                )
    else:
        return application.send_static_file("config.html")


@application.route("/config/changes.json")
def config_changes() -> RespTuple:
    return (
        json.dumps(state.get_config_changes_legacy()),
        200,
        {"Content-Type": "application/json"},
    )


@application.route("/health")
def health() -> Response:

    down_file_exists = check_down_file_exists()
    thorough = http_request.args.get("thorough", False)

    metric_tags = {
        "down_file_exists": str(down_file_exists),
        "thorough": str(thorough),
    }

    clickhouse_health = (
        check_clickhouse(ignore_experimental=True, metric_tags=metric_tags)
        if thorough
        else True
    )
    metric_tags["clickhouse_ok"] = str(clickhouse_health)

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

    if status != 200:
        metrics.increment("healthcheck_failed", tags=metric_tags)

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
def unqualified_query_view(*, timer: Timer) -> Union[Response, str, WerkzeugResponse]:
    if http_request.method == "GET":
        return redirect(f"/{settings.DEFAULT_DATASET_NAME}/query", code=302)
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        dataset = get_dataset(body.pop("dataset", settings.DEFAULT_DATASET_NAME))
        _trace_transaction(dataset)
        return dataset_query(dataset, body, timer)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<dataset:dataset>/snql", methods=["GET", "POST"])
@util.time_request("query")
def snql_dataset_query_view(*, dataset: Dataset, timer: Timer) -> Union[Response, str]:
    if http_request.method == "GET":
        schema = RequestSchema.build(HTTPQuerySettings)
        return render_template(
            "query.html",
            query_template=json.dumps(schema.generate_template(), indent=4),
        )
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        _trace_transaction(dataset)
        return dataset_query(dataset, body, timer)
    else:
        assert False, "unexpected fallthrough"


@with_span()
def dataset_query(
    dataset: Dataset, body: MutableMapping[str, Any], timer: Timer
) -> Response:
    assert http_request.method == "POST"
    referrer = http_request.referrer or "<unknown>"  # mypy

    # Try to detect if new requests are being sent to the api
    # after the shutdown command has been issued, and if so
    # how long after. I don't want to do a disk check for
    # every query, so randomly sample until the shutdown file
    # is detected, and then log everything
    if IS_SHUTTING_DOWN or random.random() < 0.05:
        if IS_SHUTTING_DOWN or check_down_file_exists():
            tags = {"dataset": get_dataset_name(dataset)}
            metrics.increment("post.shutdown.query", tags=tags)
            diff = time.time() - (shutdown_time() or 0.0)  # this should never be None
            metrics.timing("post.shutdown.query.delay", diff, tags=tags)

    with sentry_sdk.start_span(description="build_schema", op="validate"):
        schema = RequestSchema.build(HTTPQuerySettings)

    request = build_request(
        body, parse_snql_query, HTTPQuerySettings, schema, dataset, timer, referrer
    )

    try:
        result = parse_and_run_query(dataset, request, timer)
    except QueryException as exception:
        status = 500
        details: Mapping[str, Any]

        cause = exception.__cause__
        if isinstance(cause, RateLimitExceeded):
            status = 429
            details = {
                "type": "rate-limited",
                "message": str(cause),
            }
            logger.warning(
                str(cause),
                exc_info=True,
            )
        elif isinstance(cause, ClickhouseError):
            status = get_http_status_for_clickhouse_error(cause)
            details = {
                "type": "clickhouse",
                "message": str(cause),
                "code": cause.code,
            }
        elif isinstance(cause, QueryTooLongException):
            status = 400
            details = {"type": "query-too-long", "message": str(cause)}
        elif isinstance(cause, Exception):
            details = {
                "type": "unknown",
                "message": str(cause),
            }
        else:
            raise  # exception should have been chained

        return Response(
            json.dumps(
                {"error": details, "timing": timer.for_json(), **exception.extra}
            ),
            status,
            {"Content-Type": "application/json"},
        )

    payload: MutableMapping[str, Any] = {**result.result, "timing": timer.for_json()}

    if settings.STATS_IN_RESPONSE or request.query_settings.get_debug():
        payload.update(result.extra)

    return Response(
        json.dumps(payload, default=str), 200, {"Content-Type": "application/json"}
    )


@application.errorhandler(InvalidSubscriptionError)
def handle_subscription_error(exception: InvalidSubscriptionError) -> Response:
    data = {"error": {"type": "subscription", "message": str(exception)}}
    return Response(
        json.dumps(data, indent=4),
        400,
        {"Content-Type": "application/json"},
    )


@application.route("/<dataset:dataset>/<entity:entity>/subscriptions", methods=["POST"])
@util.time_request("subscription")
def create_subscription(*, dataset: Dataset, timer: Timer, entity: Entity) -> RespTuple:
    if entity not in dataset.get_all_entities():
        raise InvalidSubscriptionError(
            "Invalid subscription dataset and entity combination"
        )
    entity_key = get_entity_name(entity)
    subscription = SubscriptionDataCodec(entity_key).decode(http_request.data)
    identifier = SubscriptionCreator(dataset, entity_key).create(subscription, timer)

    metrics.increment("subscription_created", tags={"entity": entity_key.value})
    return (
        json.dumps({"subscription_id": str(identifier)}),
        202,
        {"Content-Type": "application/json"},
    )


@application.route(
    "/<dataset:dataset>/<entity:entity>/subscriptions/<int:partition>/<key>",
    methods=["DELETE"],
)
def delete_subscription(
    *, dataset: Dataset, partition: int, key: str, entity: Entity
) -> RespTuple:
    if entity not in dataset.get_all_entities():
        raise InvalidSubscriptionError(
            "Invalid subscription dataset and entity combination"
        )
    entity_key = get_entity_name(entity)
    SubscriptionDeleter(entity_key, PartitionId(partition)).delete(UUID(key))
    metrics.increment("subscription_deleted", tags={"entity": entity_key.value})

    return "ok", 202, {"Content-Type": "text/plain"}


if application.debug or application.testing:
    # These should only be used for testing/debugging. Note that the database name
    # is checked to avoid scary production mishaps.

    from snuba.datasets.entity import Entity as EntityType
    from snuba.web.converters import EntityConverter

    application.url_map.converters["entity"] = EntityConverter

    def _write_to_entity(*, entity: EntityType) -> RespTuple:
        from snuba.processor import InsertBatch

        rows: MutableSequence[WriterTableRow] = []
        offset_base = int(round(time.time() * 1000))
        writable_storage = entity.get_writable_storage()
        assert writable_storage is not None
        table_writer = writable_storage.get_table_writer()

        for index, message in enumerate(json.loads(http_request.data)):
            offset = offset_base + index

            processed_message = (
                table_writer.get_stream_loader()
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
            table_writer.get_batch_writer(metrics),
            JSONRowEncoder(),
        ).write(rows)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/entities/<entity:entity>/insert", methods=["POST"])
    def write_to_entity(*, entity: EntityType) -> RespTuple:
        return _write_to_entity(entity=entity)

    @application.route("/tests/<entity:entity>/eventstream", methods=["POST"])
    def eventstream(*, entity: Entity) -> RespTuple:
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

        storage = entity.get_writable_storage()
        assert storage is not None

        if type_ == "insert":
            from arroyo.processing.strategies.factory import (
                KafkaConsumerStrategyFactory,
            )

            from snuba.consumers.consumer import build_batch_writer, process_message

            table_writer = storage.get_table_writer()
            stream_loader = table_writer.get_stream_loader()
            strategy = KafkaConsumerStrategyFactory(
                stream_loader.get_pre_filter(),
                functools.partial(
                    process_message, stream_loader.get_processor(), "consumer_grouup"
                ),
                build_batch_writer(table_writer, metrics=metrics),
                max_batch_size=1,
                max_batch_time=1.0,
                processes=None,
                input_block_size=None,
                output_block_size=None,
            ).create_with_partitions(lambda offsets: None, {})
            strategy.submit(message)
            strategy.close()
            strategy.join()
        else:
            from snuba.replacer import ReplacerWorker

            worker = ReplacerWorker(storage, "consumer_group", metrics=metrics)
            processed = worker.process_message(message)
            if processed is not None:
                batch = [processed]
                worker.flush_batch(batch)

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/<dataset:dataset>/drop", methods=["POST"])
    def drop(*, dataset: Dataset) -> RespTuple:
        truncate_dataset(dataset)
        for redis_client in all_redis_clients():
            redis_client.flushdb()

        return ("ok", 200, {"Content-Type": "text/plain"})

    @application.route("/tests/error")
    def error() -> RespTuple:
        1 / 0
        # unreachable. A valid response is added for mypy
        return ("error", 500, {"Content-Type": "text/plain"})
