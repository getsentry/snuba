from __future__ import annotations

import logging
from typing import Optional

from snuba import environment
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_execution import ExecutionStage
from snuba.pipeline.stages.query_processing import EntityAndStoragePipelineStage
from snuba.query.exceptions import QueryPlanException
from snuba.querylog import record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.request import Request
from snuba.utils.metrics.gauge import Gauge
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.util import with_span
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryExtraData, QueryResult

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")

MAX_QUERY_SIZE_BYTES = 256 * 1024  # 256 KiB by default


def _run_query_pipeline(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    robust: bool = False,
    concurrent_queries_gauge: Optional[Gauge] = None,
    force_dry_run: bool = False,
) -> QueryResult:
    clickhouse_query = EntityAndStoragePipelineStage().execute(
        QueryPipelineResult(
            data=request, query_settings=request.query_settings, timer=timer, error=None
        )
    )
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
def parse_and_run_query(
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
