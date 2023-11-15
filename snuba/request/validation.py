from __future__ import annotations

import logging
import random
import textwrap
import uuid
from typing import Any, Dict, MutableMapping, Optional, Protocol, Tuple, Type, Union

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
from snuba.querylog.query_metadata import get_request_status
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.schema import RequestParts, RequestSchema
from snuba.utils.metrics.timer import Timer

logger = logging.getLogger("snuba.request.validation")


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
    return _parse_snql_query(
        request_parts.query["query"], dataset, custom_processing, settings
    )


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


def update_attribution_info(
    request_parts: RequestParts, referrer: str, query_project_id: int
) -> dict[str, str]:
    attribution_info = dict(request_parts.attribution_info)
    attribution_info["app_id"] = get_app_id(request_parts.attribution_info["app_id"])

    if "referrer" not in request_parts.attribution_info["tenant_ids"]:
        tenant_ids_referrer = "<unknown>"
    else:
        tenant_ids_referrer = request_parts.attribution_info["tenant_ids"]["referrer"]

    if referrer != tenant_ids_referrer:
        logger.info(
            f"Received request contains different referrers: {referrer} != {tenant_ids_referrer}. Default to tenant_ids referrer."
        )

    if referrer != "<unknown>":
        attribution_info["referrer"] = referrer
    else:
        attribution_info["referrer"] = tenant_ids_referrer

    attribution_info["tenant_ids"] = request_parts.attribution_info["tenant_ids"]
    if (
        "project_id" not in attribution_info["tenant_ids"]
        and query_project_id is not None
    ):
        attribution_info["tenant_ids"]["project_id"] = query_project_id

    return attribution_info


def build_request(
    body: Dict[str, Any],
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
                settings_obj: Union[HTTPQuerySettings, SubscriptionQuerySettings]
                # the parameters accept either `str` or `bool` but we pass in `str | bool`
                settings_obj = settings_class(**query_settings)  # type: ignore
            elif settings_class == SubscriptionQuerySettings:
                settings_obj = settings_class(
                    consistent=_consistent_override(True, referrer),
                )
            query, snql_anonymized = parser(
                request_parts, settings_obj, dataset, custom_processing
            )

            project_ids = get_object_ids_in_query_ast(query, "project_id")
            query_project_id = None
            if project_ids is not None and len(project_ids) == 1:
                query_project_id = project_ids.pop()
                sentry_sdk.set_tag("snuba_project_id", query_project_id)

            org_ids = get_object_ids_in_query_ast(query, "org_id")
            if org_ids is not None and len(org_ids) == 1:
                sentry_sdk.set_tag("snuba_org_id", org_ids.pop())

            attribution_info = update_attribution_info(
                request_parts, referrer, query_project_id
            )
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
            request_status = get_request_status(exception)
            record_invalid_request(
                timer, request_status, referrer, str(type(exception).__name__)
            )
            raise exception
        except Exception as exception:
            request_status = get_request_status(exception)
            record_error_building_request(
                timer, request_status, referrer, str(type(exception).__name__)
            )
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
        sentry_sdk.add_breadcrumb(
            category="query_info",
            level="info",
            message="snuba_query_raw",
            data={"request_id": request.id},
        )

        timer.mark("validate_schema")
        return request
