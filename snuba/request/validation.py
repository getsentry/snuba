from __future__ import annotations

import random
import textwrap
import uuid
from typing import Any, MutableMapping, Optional, Protocol, Tuple, Type, Union

import sentry_sdk

from snuba import state
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.dataset import Dataset
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query
from snuba.query.query_settings import (
    HTTPQuerySettings,
    QuerySettings,
    SubscriptionQuerySettings,
)
from snuba.query.snql.parser import CustomProcessors
from snuba.query.snql.parser import parse_snql_query as _parse_snql_query
from snuba.querylog import record_error_building_request, record_invalid_request
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.schema import RequestParts, RequestSchema
from snuba.utils.metrics.timer import Timer


class Parser(Protocol):
    def __call__(
        self,
        request_parts: RequestParts,
        settings: QuerySettings,
        dataset: Dataset,
        custom_processing: Optional[CustomProcessors] = ...,
    ) -> Tuple[Union[Query, CompositeQuery[Entity]], str]:
        ...


def parse_snql_query(
    request_parts: RequestParts,
    settings: QuerySettings,
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
    settings_class: Union[Type[HTTPQuerySettings], Type[SubscriptionQuerySettings]],
    schema: RequestSchema,
    dataset: Dataset,
    timer: Timer,
    referrer: str,
    custom_processing: Optional[CustomProcessors] = None,
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        try:
            request_parts = schema.validate(body)
            if settings_class == HTTPQuerySettings:
                query_settings: MutableMapping[str, bool | str] = {
                    **request_parts.query_settings,
                    "consistent": _consistent_override(
                        request_parts.query_settings.get("consistent", False), referrer
                    ),
                }
                query_settings["referrer"] = referrer
                # TODO: referrer probably doesn't need to be passed in, it should be from the body
                settings_obj: Union[
                    HTTPQuerySettings, SubscriptionQuerySettings
                ] = settings_class(
                    **query_settings,
                )
            elif settings_class == SubscriptionQuerySettings:
                settings_obj = settings_class(
                    consistent=_consistent_override(True, referrer),
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
            attribution_info = dict(request_parts.attribution_info)
            # TODO: clean this up
            attribution_info["app_id"] = get_app_id(
                request_parts.attribution_info["app_id"]
            )
            attribution_info["referrer"] = referrer

            request_id = uuid.uuid4().hex
            request = Request(
                id=request_id,
                # TODO: Replace this with the actual query raw body.
                # this can have an impact on subscriptions so we need
                # to be careful with the change.
                original_body=body,
                query=query,
                attribution_info=AttributionInfo(**attribution_info),
                query_settings=settings_obj,
                snql_anonymized=snql_anonymized,
            )
        except (InvalidJsonRequestException, InvalidQueryException) as exception:
            record_invalid_request(timer, referrer)
            raise exception
        except Exception as exception:
            record_error_building_request(timer, referrer)
            raise exception

        span.set_data(
            "snuba_query_parsed",
            repr(query).split("\n"),
        )
        span.set_data(
            "snuba_query_raw",
            textwrap.wrap(repr(request.original_body), 100, break_long_words=False),
        )
        sentry_sdk.add_breadcrumb(
            category="query_info",
            level="info",
            message="snuba_query_raw",
            data={
                "query": textwrap.wrap(
                    repr(request.original_body), 100, break_long_words=False
                )
            },
        )

        timer.mark("validate_schema")
        return request
