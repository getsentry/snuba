from __future__ import annotations

import atexit
import functools
import logging
import random
import time
from datetime import datetime
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
from arroyo.backends.kafka import KafkaPayload
from arroyo.types import BrokerValue, Message, Partition, Topic
from flask import (
    Flask,
    Request,
    Response,
    jsonify,
    make_response,
    redirect,
    render_template,
)
from flask import request as http_request
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from werkzeug import Response as WerkzeugResponse
from werkzeug.exceptions import InternalServerError

from snuba import settings, util
from snuba.clickhouse.errors import ClickhouseError
from snuba.clickhouse.http import JSONRowEncoder
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.cogs.accountant import close_cogs_recorder
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity_name
from snuba.datasets.entity import Entity
from snuba.datasets.entity_subscriptions.validators import InvalidSubscriptionError
from snuba.datasets.factory import InvalidDatasetError, get_dataset_name
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import StorageNotAvailable, WritableTableStorage
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.query.exceptions import InvalidQueryException, QueryPlanException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.redis import all_redis_clients
from snuba.request.exceptions import InvalidJsonRequestException, JsonDecodeException
from snuba.request.schema import RequestSchema
from snuba.state.rate_limit import RateLimitExceeded
from snuba.subscriptions.codecs import SubscriptionDataCodec
from snuba.subscriptions.data import PartitionId
from snuba.subscriptions.subscription import SubscriptionCreator, SubscriptionDeleter
from snuba.utils.health_info import (
    check_down_file_exists,
    get_health_info,
    get_shutdown,
    metrics,
    shutdown_time,
)
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.web import QueryException, QueryTooLongException
from snuba.web.bulk_delete_query import delete_from_storage as bulk_delete_from_storage
from snuba.web.constants import get_http_status_for_clickhouse_error
from snuba.web.converters import DatasetConverter, EntityConverter, StorageConverter
from snuba.web.delete_query import DeletesNotEnabledError, TooManyOngoingMutationsError
from snuba.web.query import parse_and_run_query
from snuba.web.rpc import run_rpc_handler
from snuba.writer import BatchWriterEncoderWrapper, WriterTableRow

logger = logging.getLogger("snuba.api")

# Flask wants a Dict, not a Mapping
RespTuple = Tuple[Text, int, Dict[Any, Any]]


def truncate_dataset(dataset: Dataset) -> None:
    for entity in dataset.get_all_entities():
        for storage in entity.get_all_storages():
            cluster = storage.get_cluster()
            nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
            for node in nodes:
                clickhouse = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)

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
application.url_map.converters["storage"] = StorageConverter
atexit.register(close_cogs_recorder)


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
        json.dumps({"error": {"type": "invalid_query", "message": str(exception)}}, indent=4),
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

    if not down_file_exists:
        status = 200
    else:
        status = 503

    if status != 200:
        metrics.increment("healthcheck_envoy_failed")
    return Response(json.dumps({}), status, {"Content-Type": "application/json"})


@application.route("/health")
def health() -> Response:
    thorough = http_request.args.get("thorough", False)
    health_info = get_health_info(thorough)

    return Response(health_info.body, health_info.status, health_info.content_type)


def parse_request_body(http_request: Request) -> Dict[str, Any]:
    with sentry_sdk.start_span(name="parse_request_body", op="parse"):
        metrics.timing("http_request_body_length", len(http_request.data))
        try:
            body = json.loads(http_request.data)
            assert isinstance(body, Dict)
            return body
        except json.JSONDecodeError as error:
            raise JsonDecodeException(str(error)) from error


def _trace_transaction(dataset_name: str) -> None:
    span = sentry_sdk.get_current_span()
    if span:
        span.set_tag("dataset", dataset_name)
        span.set_tag("referrer", http_request.referrer)

    scope = sentry_sdk.get_current_scope()
    if scope.transaction:
        scope.set_transaction_name(
            f"{scope.transaction.name}__{dataset_name}__{http_request.referrer}"
        )
        scope.transaction.set_tag("dataset", dataset_name)
        scope.transaction.set_tag("referrer", http_request.referrer)


@application.route("/query", methods=["GET", "POST"])
@util.time_request("query")
def unqualified_query_view(*, timer: Timer) -> Union[Response, str, WerkzeugResponse]:
    if http_request.method == "GET":
        return redirect(f"/{settings.DEFAULT_DATASET_NAME}/query", code=302)
    elif http_request.method == "POST":
        body = parse_request_body(http_request)
        dataset_name = str(body.pop("dataset", settings.DEFAULT_DATASET_NAME))
        _trace_transaction(dataset_name)
        return dataset_query(dataset_name, body, timer)
    else:
        assert False, "unexpected fallthrough"


@application.route("/rpc/<name>/<version>", methods=["POST"])
def rpc(*, name: str, version: str) -> Response:
    result_proto = run_rpc_handler(name, version, http_request.data)
    if isinstance(result_proto, ErrorProto):
        return Response(result_proto.SerializeToString(), status=result_proto.code)
    else:
        return Response(result_proto.SerializeToString(), status=200)


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
        dataset_name = get_dataset_name(dataset)
        _trace_transaction(dataset_name)
        return dataset_query(dataset_name, body, timer)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<dataset:dataset>/mql", methods=["GET", "POST"])
@util.time_request("query", {"mql": "true"})
def mql_dataset_query_view(*, dataset: Dataset, timer: Timer) -> Union[Response, str]:
    if http_request.method == "POST":
        dataset_name = get_dataset_name(dataset)
        body = parse_request_body(http_request)
        _trace_transaction(dataset_name)
        return dataset_query(dataset_name, body, timer, is_mql=True)
    else:
        assert False, "unexpected fallthrough"


@application.route("/<storage:storage>", methods=["DELETE"])
@util.time_request("delete_query")
def storage_delete(*, storage: WritableTableStorage, timer: Timer) -> Union[Response, str]:
    if http_request.method == "DELETE":
        check_shutdown({"storage": storage.get_storage_key()})
        body = parse_request_body(http_request)

        try:
            schema = RequestSchema.build(HTTPQuerySettings, is_delete=True)
            request_parts = schema.validate(body)
            payload = bulk_delete_from_storage(
                storage,
                request_parts.query["query"]["columns"],
                request_parts.attribution_info,
            )
        except (
            InvalidJsonRequestException,
            DeletesNotEnabledError,
            InvalidQueryException,
        ) as error:
            details = {
                "type": "invalid_query",
                "message": str(error),
            }
            return make_response(
                jsonify({"error": details}),
                400,
            )
        except TooManyOngoingMutationsError as e:
            details = {
                "type": "too_many_ongoing_mutations",
                "message": str(e),
            }
            return make_response(
                jsonify({"error": details}),
                503,
            )
        except Exception as error:
            logger.warning("Failed query", exc_info=error)
            details = details = {
                "type": "unknown",
                "message": str(error),
            }
            return make_response(jsonify({"error": details}), 500)

        # i put the result inside "data" bc thats how sentry utils/snuba.py expects the result
        return Response(dump_payload({"data": payload}), 200, {"Content-Type": "application/json"})

    else:
        assert False, "unexpected fallthrough"


def _sanitize_payload(payload: MutableMapping[str, Any], res: MutableMapping[str, Any]) -> None:
    def hex_encode_if_bytes(value: Any) -> Any:
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                # encode the byte string in a hex string
                return "RAW_BYTESTRING__" + value.hex()

        return value

    for k, v in payload.items():
        if isinstance(v, dict):
            res[hex_encode_if_bytes(k)] = {}
            _sanitize_payload(v, res[hex_encode_if_bytes(k)])
        elif isinstance(v, list):
            res[hex_encode_if_bytes(k)] = []
            for item in v:
                if isinstance(item, dict):
                    res[hex_encode_if_bytes(k)].append({})
                    _sanitize_payload(item, res[hex_encode_if_bytes(k)][-1])
                else:
                    res[hex_encode_if_bytes(k)].append(hex_encode_if_bytes(item))
        else:
            res[hex_encode_if_bytes(k)] = hex_encode_if_bytes(v)


def dump_payload(payload: MutableMapping[str, Any]) -> str:
    try:
        return json.dumps(payload, default=str)
    except UnicodeDecodeError:
        # If there were any string that could not be decoded, we
        # encode the problematic bytes in a hex string.
        # this is to prevent other clients downstream of us from having
        # to deal with potentially malicious strings and to prevent one
        # bad string from breaking the entire payload.
        sanitized_payload: MutableMapping[str, Any] = {}
        _sanitize_payload(payload, sanitized_payload)
        return json.dumps(sanitized_payload, default=str)


def check_shutdown(tags: Dict[str, Any]) -> None:
    # Try to detect if new requests are being sent to the api
    # after the shutdown command has been issued, and if so
    # how long after. I don't want to do a disk check for
    # every query, so randomly sample until the shutdown file
    # is detected, and then log everything
    if get_shutdown() or random.random() < 0.05:
        if get_shutdown() or check_down_file_exists():
            metrics.increment("post.shutdown.query", tags=tags)
            diff = time.time() - (shutdown_time() or 0.0)  # this should never be None
            metrics.timing("post.shutdown.query.delay", diff, tags=tags)


@with_span()
def dataset_query(
    dataset_name: str, body: Dict[str, Any], timer: Timer, is_mql: bool = False
) -> Response:
    assert http_request.method == "POST"
    referrer = http_request.referrer or "<unknown>"  # mypy

    check_shutdown({"dataset": dataset_name})

    try:
        request, result = parse_and_run_query(body, timer, is_mql, dataset_name, referrer)
        assert result.extra["stats"]
    except InvalidQueryException as exception:
        details: Mapping[str, Any]
        details = {
            "type": "invalid_query",
            "message": str(exception),
        }
        return Response(
            json.dumps(
                {"error": details},
            ),
            400,
            {"Content-Type": "application/json"},
        )
    except QueryException as exception:
        status = 500
        cause = exception.__cause__
        if isinstance(cause, (RateLimitExceeded, AllocationPolicyViolations)):
            status = 429
            details = {
                "type": "rate-limited",
                "message": str(cause),
            }
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
                {
                    "error": details,
                    "timing": timer.for_json(),
                    "quota_allowance": getattr(cause, "quota_allowance", {}),
                    **exception.extra,
                }
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
    payload: MutableMapping[str, Any] = {
        **result.result,
        "timing": timer.for_json(),
        "quota_allowance": result.quota_allowance,
    }

    if settings.STATS_IN_RESPONSE or request.query_settings.get_debug():
        payload.update(result.extra)

    return Response(dump_payload(payload), 200, {"Content-Type": "application/json"})


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
        raise InvalidSubscriptionError("Invalid subscription dataset and entity combination")
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
def delete_subscription(*, dataset: Dataset, partition: int, key: str, entity: Entity) -> RespTuple:
    if entity not in dataset.get_all_entities():
        raise InvalidSubscriptionError("Invalid subscription dataset and entity combination")
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

    def _write_to_entity(
        *,
        entity: EntityType,
    ) -> RespTuple:
        from snuba.processor import InsertBatch

        rows: MutableSequence[WriterTableRow] = []
        offset_base = int(round(time.time() * 1000))
        writable_storage = entity.get_writable_storage()
        assert writable_storage is not None
        table_writer = writable_storage.get_table_writer()

        if http_request.files:
            messages = [file.read() for _, file in http_request.files.items()]
        else:
            messages = json.loads(http_request.data)

        for index, message in enumerate(messages):
            offset = offset_base + index
            processed_message = (
                table_writer.get_stream_loader()
                .get_processor()
                .process_message(
                    message,
                    KafkaMessageMetadata(offset=offset, partition=0, timestamp=datetime.utcnow()),
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
    def write_json_to_entity(*, entity: EntityType) -> RespTuple:
        return _write_to_entity(
            entity=entity,
        )

    @application.route("/tests/entities/<entity:entity>/insert_bytes", methods=["POST"])
    def write_bytes_to_entity(*, entity: EntityType) -> RespTuple:
        return _write_to_entity(
            entity=entity,
        )

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
        should_validate_schema = True

        try:
            if type_ == "insert":
                from snuba.consumers.consumer import build_batch_writer, process_message
                from snuba.consumers.strategy_factory import (
                    KafkaConsumerStrategyFactory,
                )

                table_writer = storage.get_table_writer()
                stream_loader = table_writer.get_stream_loader()

                def commit(offsets: Mapping[Partition, int], force: bool = False) -> None:
                    pass

                strategy = KafkaConsumerStrategyFactory(
                    stream_loader.get_pre_filter(),
                    functools.partial(
                        process_message,
                        stream_loader.get_processor(),
                        "consumer_grouup",
                        stream_loader.get_default_topic_spec().topic,
                        should_validate_schema,
                    ),
                    build_batch_writer(table_writer, metrics=metrics),
                    max_batch_size=1,
                    max_batch_time=1.0,
                    processes=None,
                    input_block_size=None,
                    output_block_size=None,
                    max_insert_batch_size=None,
                    max_insert_batch_time=None,
                    metrics_tags={},
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
