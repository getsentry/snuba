from __future__ import annotations

import logging
from typing import Any, Optional

import sentry_sdk

from snuba import environment, settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import InvalidDatasetError, get_dataset, get_dataset_name
from snuba.datasets.pluggable_dataset import PluggableDataset
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_execution import ExecutionStage
from snuba.pipeline.stages.query_processing import (
    EntityProcessingStage,
    StorageProcessingStage,
)
from snuba.query.exceptions import InvalidQueryException, QueryPlanException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.querylog import record_invalid_request, record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata, get_request_status
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_mql_query, parse_snql_query
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryExtraData, QueryResult

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")


def _run_query_pipeline(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    robust: bool = False,
    concurrent_queries_gauge: Optional[Gauge] = None,
    force_dry_run: bool = False,
) -> QueryResult:
    clickhouse_query = EntityProcessingStage().execute(
        QueryPipelineResult(
            data=request,
            query_settings=request.query_settings,
            timer=timer,
            error=None,
        )
    )
    print("roudn1", clickhouse_query)
    clickhouse_query = StorageProcessingStage().execute(clickhouse_query)
    print("round2", clickhouse_query)

    res = ExecutionStage(
        request.attribution_info,
        query_metadata=query_metadata,
        robust=robust,
        concurrent_queries_gauge=concurrent_queries_gauge,
    ).execute(clickhouse_query)
    if res.error:
        raise res.error
    elif res.data:
        return res.data
    # we should never get here
    raise Exception("No result or data, very bad exception")


@with_span()
def run_query(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    robust: bool = False,
    concurrent_queries_gauge: Optional[Gauge] = None,
) -> QueryResult:
    """
    Processes, runs a Snuba Query, then records the metadata about the query that was run.
    """
    query_metadata = SnubaQueryMetadata(request, get_dataset_name(dataset), timer)

    try:
        result = _run_query_pipeline(
            request, timer, query_metadata, robust, concurrent_queries_gauge
        )
        if not request.query_settings.get_dry_run():
            record_query(request, timer, query_metadata, result)
        _set_query_final(request, result.extra)
    except InvalidQueryException as error:
        request_status = get_request_status(error)
        record_invalid_request(
            request_id=request.id,
            body=request.original_body,
            dataset=get_dataset_name(dataset),
            organization=int(
                request.attribution_info.tenant_ids.get("organization_id", 0)
            ),
            timer=timer,
            request_status=request_status,
            referrer=request.attribution_info.referrer,
            exception_name=str(type(error).__name__),
        )
        raise error
    except QueryException as error:
        _set_query_final(request, error.extra)
        record_query(request, timer, query_metadata, error)
        raise error
    except QueryPlanException as error:
        record_query(request, timer, query_metadata, error)
        raise error

    return result


def _get_dataset(dataset_name: Optional[str]) -> Dataset:
    if dataset_name:
        try:
            return get_dataset(dataset_name)
        except InvalidDatasetError:
            return PluggableDataset(name=dataset_name, all_entities=[])
    return PluggableDataset(name=settings.DEFAULT_DATASET_NAME, all_entities=[])


@with_span()
def parse_and_run_query(
    body: dict[str, Any],
    timer: Timer,
    is_mql: bool = False,
    dataset_name: Optional[str] = None,
    referrer: Optional[str] = None,
) -> tuple[Request, QueryResult]:
    """Top level entrypoint from a raw query body to a query result

    Example:
    >>> request, result = parse_and_run_query(
    >>>     body={
    >>>         "query":"MATCH (events) SELECT event_id, group_id, project_id, timestamp WHERE timestamp >= toDateTime('2024-07-17T21:04:34') AND timestamp < toDateTime('2024-07-17T21:10:34')",
    >>>         "tenant_ids":{"organization_id":319976,"referrer":"Group.get_helpful"}
    >>>     },
    >>>     timer=Timer("parse_and_run_query"),
    >>>     is_mql=False,
    >>> )

    Optional args:
        dataset_name (str): used mainly for observability purposes
        referrer (str): legacy param, you probably don't need to provide this. It should be in the tenant_ids of the body
    """

    with sentry_sdk.start_span(description="build_schema", op="validate"):
        schema = RequestSchema.build(HTTPQuerySettings, is_mql)

    # NOTE(Volo): dataset is not necessary for queries because storages can be queried directly
    # certain parts of the code still use it though, many metrics used by snuba are still tagged by
    # "dataset" even though datasets don't define very much. The user is able to provide a dataset_name
    # for that reason. Otherwise they are not useful.
    # EXCEPT FOR DISCOVER which is a whole can of worms, but that's the one place where the dataset is useful for something
    dataset = _get_dataset(dataset_name)
    referrer = referrer or "<unknown>"
    parse_function = parse_snql_query if not is_mql else parse_mql_query
    request = build_request(
        body, parse_function, HTTPQuerySettings, schema, dataset, timer, referrer
    )

    return request, run_query(dataset, request, timer)


def _set_query_final(request: Request, extra: QueryExtraData) -> None:
    if "final" in extra["stats"]:
        request.query.set_final(extra["stats"]["final"])
