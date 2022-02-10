from __future__ import annotations

import random
import textwrap
import uuid
from typing import Any, Mapping, MutableMapping, Optional, Protocol, Tuple, Type, Union

import sentry_sdk

from snuba import state
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.dataset import Dataset
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query
from snuba.query.snql.parser import CustomProcessors
from snuba.query.snql.parser import parse_snql_query as _parse_snql_query
from snuba.querylog import record_error_building_request, record_invalid_request
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.request_settings import (
    HTTPRequestSettings,
    RequestSettings,
    SubscriptionRequestSettings,
)
from snuba.request.schema import RequestParts, RequestSchema
from snuba.utils.metrics.timer import Timer


class Parser(Protocol):
    def __call__(
        self,
        request_parts: RequestParts,
        settings: RequestSettings,
        dataset: Dataset,
        custom_processing: Optional[CustomProcessors] = ...,
    ) -> Tuple[Union[Query, CompositeQuery[Entity]], str]:
        ...


def parse_snql_query(
    request_parts: RequestParts,
    settings: RequestSettings,
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
) -> Tuple[Union[Query, CompositeQuery[Entity]], str]:
    return _parse_snql_query(request_parts.query["query"], dataset, custom_processing)


def _consistent_override(original_setting: bool, referrer: str) -> bool:
    consistent_config = state.get_config("consistent_override", None)
    if isinstance(consistent_config, str):
        referrers_override = consistent_config.split(";")
        for config in referrers_override:
            referrer_config, percentage = config.split("=")
            if referrer_config == referrer:
                if random.random() > float(percentage):
                    return False

    return original_setting


def build_request(
    body: MutableMapping[str, Any],
    parser: Parser,
    settings_class: Union[Type[HTTPRequestSettings], Type[SubscriptionRequestSettings]],
    schema: RequestSchema,
    dataset: Dataset,
    timer: Timer,
    referrer: str,
    custom_processing: Optional[CustomProcessors] = None,
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        try:
            request_parts = schema.validate(body)
            if settings_class == HTTPRequestSettings:
                settings: Mapping[str, bool | str] = {
                    **request_parts.settings,
                    "consistent": _consistent_override(
                        request_parts.settings.get("consistent", False), referrer
                    ),
                }
                settings_obj: Union[
                    HTTPRequestSettings, SubscriptionRequestSettings
                ] = settings_class(
                    referrer=referrer,
                    parent_api=request_parts.query["parent_api"],
                    **settings,
                )
            elif settings_class == SubscriptionRequestSettings:
                settings_obj = settings_class(
                    referrer=referrer, consistent=_consistent_override(True, referrer),
                )

            query, snql_anonymized = parser(
                request_parts, settings_obj, dataset, custom_processing
            )

            project_ids = get_object_ids_in_query_ast(query, "project_id")
            if project_ids is not None and len(project_ids) == 1:
                sentry_sdk.set_tag("snuba_project_id", project_ids.pop())

            org_ids = get_object_ids_in_query_ast(query, "org_id")
            if org_ids is not None and len(org_ids) == 1:
                sentry_sdk.set_tag("snuba_org_id", org_ids.pop())

            request_id = uuid.uuid4().hex
            request = Request(
                request_id,
                # TODO: Replace this with the actual query raw body.
                # this can have an impact on subscriptions so we need
                # to be careful with the change.
                request_parts.query,
                query,
                snql_anonymized,
                settings_obj,
            )
        except (InvalidJsonRequestException, InvalidQueryException) as exception:
            record_invalid_request(timer, referrer)
            raise exception
        except Exception as exception:
            record_error_building_request(timer, referrer)
            raise exception

        span.set_data(
            "snuba_query_parsed", repr(query).split("\n"),
        )
        span.set_data(
            "snuba_query_raw",
            textwrap.wrap(repr(request.body), 100, break_long_words=False),
        )
        sentry_sdk.add_breadcrumb(
            category="query_info",
            level="info",
            message="snuba_query_raw",
            data={
                "query": textwrap.wrap(repr(request.body), 100, break_long_words=False)
            },
        )

        timer.mark("validate_schema")
        return request
