import uuid
from typing import Any, Callable, ChainMap, MutableMapping, Sequence, Type, Union

import sentry_sdk

from snuba.datasets.dataset import Dataset
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query
from snuba.query.parser import parse_query
from snuba.query.snql.parser import parse_snql_query
from snuba.querylog import record_error_building_request, record_invalid_request
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.request_settings import (
    HTTPRequestSettings,
    RequestSettings,
    SubscriptionRequestSettings,
)
from snuba.request.schema import RequestParts, RequestSchema, apply_query_extensions
from snuba.utils.metrics.timer import Timer

Parser = Callable[[RequestParts, RequestSettings], Union[Query, CompositeQuery[Entity]]]


def build_snql_parser(
    custom_processing: Sequence[Callable[[Union[CompositeQuery[Entity], Query]], None]],
) -> Parser:
    def parse(
        request_parts: RequestParts, settings: RequestSettings
    ) -> Union[Query, CompositeQuery[Entity]]:
        return parse_snql_query(request_parts.query["query"], custom_processing)

    return parse


def build_legacy_parser(dataset: Dataset) -> Parser:
    def parse(
        request_parts: RequestParts, settings: RequestSettings
    ) -> Union[Query, CompositeQuery[Entity]]:
        query = parse_query(request_parts.query, dataset)
        apply_query_extensions(query, request_parts.extensions, settings)
        return query

    return parse


def build_request(
    body: MutableMapping[str, Any],
    parser: Parser,
    settings_class: Type[RequestSettings],
    schema: RequestSchema,
    timer: Timer,
    referrer: str,
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        try:
            request_parts = schema.validate(body)
            if isinstance(settings_class, type(HTTPRequestSettings)):
                settings_obj: Union[
                    HTTPRequestSettings, SubscriptionRequestSettings
                ] = settings_class(**request_parts.settings)
            elif isinstance(settings_class, type(SubscriptionRequestSettings)):
                settings_obj = settings_class()

            query = parser(request_parts, settings_obj)

            request_id = uuid.uuid4().hex
            request = Request(
                request_id,
                # TODO: Replace this with the actual query raw body.
                # this can have an impact on subscriptions so we need
                # to be careful with the change.
                ChainMap(request_parts.query, *request_parts.extensions.values()),
                query,
                settings_obj,
                referrer,
            )
        except (InvalidJsonRequestException, InvalidQueryException) as exception:
            record_invalid_request(timer, referrer)
            raise exception
        except Exception as exception:
            record_error_building_request(timer, referrer)
            raise exception

        span.set_data("snuba_query", request.body)

        timer.mark("validate_schema")
        return request
