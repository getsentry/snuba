from __future__ import annotations

import random
import textwrap
import uuid
from typing import Any, Dict, MutableMapping, Optional, Protocol, Type, Union

import sentry_sdk

from snuba import environment, settings, state
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import LogicalDataSource
from snuba.query.exceptions import InvalidQueryException
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query as _parse_mql_query
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
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "snuba.validation")


class Parser(Protocol):
    def __call__(
        self,
        request_parts: RequestParts,
        settings: QuerySettings,
        dataset: Dataset,
        custom_processing: Optional[CustomProcessors] = ...,
    ) -> Union[Query, CompositeQuery[LogicalDataSource]]: ...


def parse_snql_query(
    request_parts: RequestParts,
    settings: QuerySettings,
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
) -> Union[Query, CompositeQuery[LogicalDataSource]]:
    return _parse_snql_query(request_parts.query["query"], dataset, custom_processing, settings)


def parse_mql_query(
    request_parts: RequestParts,
    settings: QuerySettings,
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
) -> Union[Query, CompositeQuery[LogicalDataSource]]:
    return _parse_mql_query(
        request_parts.query["query"],
        request_parts.query["mql_context"],
        dataset,
        custom_processing,
        settings,
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
    request_parts: RequestParts, referrer: str, query_project_id: Optional[int]
) -> dict[str, Any]:
    attribution_info = dict(request_parts.attribution_info)

    attribution_info["app_id"] = get_app_id(request_parts.attribution_info["app_id"])
    attribution_info["referrer"] = referrer
    attribution_info["tenant_ids"] = request_parts.attribution_info["tenant_ids"]

    if "project_id" not in attribution_info["tenant_ids"] and query_project_id is not None:
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
            dataset_name = get_dataset_name(dataset)
            if state.get_config(
                f"snql_disabled_dataset__{dataset_name}",
                dataset_name in settings.SNQL_DISABLED_DATASETS,
            ):
                raise InvalidQueryException(f"snql is disabled for dataset {dataset}")

            request_parts = schema.validate(body)
            referrer = _get_referrer(request_parts, referrer)
            settings_obj = _get_settings_object(settings_class, request_parts, referrer)
            query = parser(request_parts, settings_obj, dataset, custom_processing)
            request = _build_request(body, request_parts, referrer, settings_obj, query)
        except (InvalidJsonRequestException, InvalidQueryException) as exception:
            request_status = get_request_status(exception)
            record_invalid_request(
                request_id=uuid.uuid4(),
                body=body,
                dataset=get_dataset_name(dataset),
                organization=body.get("tenant_ids", {}).get("organization_id", 0),
                timer=timer,
                request_status=request_status,
                referrer=referrer,
                exception_name=str(type(exception).__name__),
            )
            raise exception
        except Exception as exception:
            request_status = get_request_status(exception)
            record_error_building_request(
                request_id=uuid.uuid4(),
                body=body,
                dataset=get_dataset_name(dataset),
                organization=body.get("tenant_ids", {}).get("organization_id", 0),
                timer=timer,
                request_status=request_status,
                referrer=referrer,
                exception_name=str(type(exception).__name__),
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
            data={"query": textwrap.wrap(repr(request.original_body), 100, break_long_words=False)},
        )
        sentry_sdk.add_breadcrumb(
            category="query_info",
            level="info",
            message="snuba_query_raw",
            data={"request_id": request.id},
        )

        timer.mark("validate_schema")
        return request


def _get_referrer(request_parts: RequestParts, referrer: str) -> str:
    tenant_referrer = request_parts.attribution_info["tenant_ids"].get("referrer")
    if tenant_referrer != referrer:
        metrics.increment(
            "referrer_mismatch",
            tags={
                "tenant_referrer": tenant_referrer or "none",
                "request_referrer": referrer,
            },
        )
    # Handle an edge case where the legacy endpoint is used.
    return tenant_referrer or referrer


def _get_settings_object(
    settings_class: Type[HTTPQuerySettings] | Type[SubscriptionQuerySettings],
    request_parts: RequestParts,
    referrer: str,
) -> HTTPQuerySettings | SubscriptionQuerySettings:
    if settings_class == HTTPQuerySettings:
        query_settings: MutableMapping[str, bool | str] = {
            **request_parts.query_settings,
            "consistent": _consistent_override(
                request_parts.query_settings.get("consistent", False), referrer
            ),
        }
        # TODO: referrer probably doesn't need to be passed in, it should be from the body
        query_settings["referrer"] = referrer
        # the parameters accept either `str` or `bool` but we pass in `str | bool`
        return settings_class(**query_settings)  # type: ignore
    elif settings_class == SubscriptionQuerySettings:
        return settings_class(
            consistent=_consistent_override(True, referrer),
        )
    return None  # type: ignore


def _get_project_id(query: Query | CompositeQuery[LogicalDataSource]) -> int | None:
    project_ids = get_object_ids_in_query_ast(query, "project_id")
    if project_ids is not None and len(project_ids) == 1:
        return project_ids.pop()
    return None


def _get_attribution_info(
    request_parts: RequestParts, referrer: str, query_project_id: int | None
) -> AttributionInfo:
    return AttributionInfo(**update_attribution_info(request_parts, referrer, query_project_id))


def _build_request(
    original_body: dict[str, Any],
    request_parts: RequestParts,
    referrer: str,
    settings: QuerySettings,
    query: Query | CompositeQuery[LogicalDataSource],
) -> Request:
    org_ids = get_object_ids_in_query_ast(query, "org_id")
    if org_ids is not None and len(org_ids) == 1:
        sentry_sdk.set_tag("snuba_org_id", org_ids.pop())

    query_project_id = _get_project_id(query)
    if query_project_id:
        sentry_sdk.set_tag("snuba_project_id", query_project_id)

    attribution_info = _get_attribution_info(request_parts, referrer, query_project_id)

    return Request(
        id=uuid.uuid4(),
        original_body=original_body,
        query=query,
        attribution_info=attribution_info,
        query_settings=settings,
    )
