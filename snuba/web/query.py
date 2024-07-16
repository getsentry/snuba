from __future__ import annotations

import logging
import typing
from typing import Optional

from snuba import environment
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_execution import ExecutionStage
from snuba.pipeline.stages.query_processing import (
    EntityProcessingStage,
    MaxRowsEnforcerStage,
    StorageProcessingStage,
)
from snuba.query import Query
from snuba.query.exceptions import InvalidQueryException, QueryPlanException
from snuba.querylog import record_invalid_request, record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata, get_request_status
from snuba.request import Request
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
            request_query=request.original_body.get("query", None),
        )
    )
    clickhouse_query = StorageProcessingStage().execute(clickhouse_query)

    if clickhouse_query.data is not None:
        data = typing.cast(Query, clickhouse_query.data)
        if data.get_is_delete():
            clickhouse_query = MaxRowsEnforcerStage().execute(clickhouse_query)

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
            timer,
            request_status,
            request.attribution_info.referrer,
            str(type(error).__name__),
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


def _set_query_final(request: Request, extra: QueryExtraData) -> None:
    if "final" in extra["stats"]:
        request.query.set_final(extra["stats"]["final"])
