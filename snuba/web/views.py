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
    List,
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
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic
from flask import Flask, Request, Response, redirect, render_template
from flask import request as http_request
from markdown import markdown
from werkzeug import Response as WerkzeugResponse
from werkzeug.exceptions import InternalServerError

from snuba import environment, settings, util
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
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
from snuba.datasets.factory import (
    InvalidDatasetError,
    get_dataset,
    get_dataset_name,
    get_enabled_dataset_names,
)
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import Storage, StorageNotAvailable
from snuba.query.allocation_policies import AllocationPolicyViolation
from snuba.query.exceptions import InvalidQueryException, QueryPlanException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.redis import all_redis_clients
from snuba.request.exceptions import InvalidJsonRequestException, JsonDecodeException
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.state.rate_limit import RateLimitExceeded
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
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


def filter_checked_storages() -> List[Storage]:
    datasets = [get_dataset(name) for name in get_enabled_dataset_names()]
    entities = itertools.chain(*[dataset.get_all_entities() for dataset in datasets])

    storages: List[Storage] = []
    for entity in entities:
        for storage in entity.get_all_storages():
            if storage.get_readiness_state().value in settings.SUPPORTED_STATES:
                storages.append(storage)
    return storages


def check_clickhouse(metric_tags: dict[str, Any] | None = None) -> bool:
    """
    Checks if all the tables in all the enabled datasets exist in ClickHouse
    TODO: Eventually, when we fully migrate to readiness_states, we can remove DISABLED_DATASETS.
    """
    try:
        storages = filter_checked_storages()
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
            # Be a little defensive here, since it's not always obvious this extra data
            # is being passed to metrics, and we might want non-string values in the
            # exception data.
            for k, v in err.extra_data.items():
                if isinstance(v, str):
                    metric_tags[k] = v

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


@application.route("/health_envoy")
def health_envoy() -> Response:
    """K8s can decide to shut down the pod, at which point it will write the down file.
    This down file signals that we have to drain the pod, and not accept any new traffic.
    In SaaS, envoy is the thing that will decided to route traffic to this node or not. It
    uses this endpoint to make that decision.

    This differs from the generic health endpoint because the pod can still be healthy
    but just not accepting traffic. That way k8s will not restart it until it is drained
    """

    down_file_exists = check_down_file_exists()

    body: Mapping[str, Union[str, bool]]
    if not down_file_exists:
        status = 200
    else:
        status = 503

    if status != 200:
        metrics.increment("healthcheck_envoy_failed")
    return Response(json.dumps({}), status, {"Content-Type": "application/json"})


@application.route("/health")
def health() -> Response:
    start = time.time()
    down_file_exists = check_down_file_exists()
    thorough = http_request.args.get("thorough", False)

    metric_tags = {
        "down_file_exists": str(down_file_exists),
        "thorough": str(thorough),
    }

    clickhouse_health = check_clickhouse(metric_tags=metric_tags) if thorough else True
    metric_tags["clickhouse_ok"] = str(clickhouse_health)

    body: Mapping[str, Union[str, bool]]
    if clickhouse_health:
        body = {"status": "ok", "down_file_exists": down_file_exists}
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
        logger.error(f"Snuba health check failed! Tags: {metric_tags}")
    metrics.timing(
        "healthcheck.latency", time.time() - start, tags={"thorough": str(thorough)}
    )
    return Response(json.dumps(body), status, {"Content-Type": "application/json"})


def parse_request_body(http_request: Request) -> Dict[str, Any]:
    with sentry_sdk.start_span(description="parse_request_body", op="parse"):
        metrics.timing("http_request_body_length", len(http_request.data))
        try:
            body = json.loads(http_request.data)
            assert isinstance(body, Dict)
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
def dataset_query(dataset: Dataset, body: Dict[str, Any], timer: Timer) -> Response:
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
        if isinstance(cause, (RateLimitExceeded, AllocationPolicyViolation)):
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
    except QueryPlanException as exception:
        if isinstance(exception, StorageNotAvailable):
            status = 400
            details = {
                "type": "storage-not-available",
                "message": str(exception.message),
            }
        else:
            raise  # exception should have been chained
        return Response(
            json.dumps({"error": details, "timing": timer.for_json()}),
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
            BrokerValue(
                KafkaPayload(None, http_request.data, []),
                Partition(Topic("topic"), 0),
                0,
                datetime.now(),
            )
        )

        type_ = record[1]

        storage = entity.get_writable_storage()
        assert storage is not None

        try:
            if type_ == "insert":
                from snuba.consumers.consumer import build_batch_writer, process_message
                from snuba.consumers.strategy_factory import (
                    KafkaConsumerStrategyFactory,
                )

                table_writer = storage.get_table_writer()
                stream_loader = table_writer.get_stream_loader()

                def commit(
                    offsets: Mapping[Partition, int], force: bool = False
                ) -> None:
                    pass

                strategy = KafkaConsumerStrategyFactory(
                    stream_loader.get_pre_filter(),
                    functools.partial(
                        process_message,
                        stream_loader.get_processor(),
                        "consumer_grouup",
                        stream_loader.get_default_topic_spec().topic,
                    ),
                    build_batch_writer(table_writer, metrics=metrics),
                    max_batch_size=1,
                    max_batch_time=1.0,
                    processes=None,
                    input_block_size=None,
                    output_block_size=None,
                ).create_with_partitions(commit, {})
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
        except Exception as e:
            # TODO: This is a temporary workaround so that we return a more useful error when
            # attempting to write to a dataset where the migration hasn't been run. This should be
            # no longer necessary once we have more advanced dataset management in place.
            raise InternalServerError(str(e), original_exception=e)

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
