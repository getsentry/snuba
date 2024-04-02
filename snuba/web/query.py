from __future__ import annotations

import logging
import random
from typing import Optional

from snuba import environment
from snuba import settings as snuba_settings
from snuba import state
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_execution import ExecutionStage
from snuba.pipeline.stages.query_processing import (
    EntityAndStoragePipelineStage,
    EntityProcessingStage,
    StorageProcessingStage,
)
from snuba.query.composite import CompositeQuery
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


def _run_query_pipeline(
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    robust: bool = False,
    concurrent_queries_gauge: Optional[Gauge] = None,
    force_dry_run: bool = False,
) -> QueryResult:
    try_new_query_pipeline_rollout = state.get_float_config(
        "try_new_query_pipeline", snuba_settings.TRY_NEW_QUERY_PIPELINE_SAMPLE_RATE
    )
    run_new_query_pipeline_rollout = state.get_float_config(
        "run_new_query_pipeline", snuba_settings.USE_NEW_QUERY_PIPELINE_SAMPLE_RATE
    )
    try_new_query_pipeline = (
        random.random() <= try_new_query_pipeline_rollout
        if try_new_query_pipeline_rollout is not None
        else False
    )
    run_new_query_pipeline = (
        random.random() <= run_new_query_pipeline_rollout
        if run_new_query_pipeline_rollout is not None
        else False
    )
    if isinstance(request.query, CompositeQuery):
        # New pipeline does not support composite queries yet.
        try_new_query_pipeline = False
        run_new_query_pipeline = False

    if run_new_query_pipeline:
        clickhouse_query = EntityProcessingStage().execute(
            QueryPipelineResult(
                data=request,
                query_settings=request.query_settings,
                timer=timer,
                error=None,
            )
        )
        clickhouse_query = StorageProcessingStage().execute(clickhouse_query)
    else:
        clickhouse_query = EntityAndStoragePipelineStage().execute(
            QueryPipelineResult(
                data=request,
                query_settings=request.query_settings,
                timer=timer,
                error=None,
            )
        )
        if try_new_query_pipeline:
            try:
                compare_clickhouse_query = EntityProcessingStage().execute(
                    QueryPipelineResult(
                        data=request,
                        query_settings=request.query_settings,
                        timer=timer,
                        error=None,
                    )
                )
                compare_clickhouse_query = StorageProcessingStage().execute(
                    clickhouse_query
                )
                new_sql = str(compare_clickhouse_query.data)
                old_sql = str(clickhouse_query.data)
                if new_sql != old_sql:
                    logger.warning(
                        "New and old query pipeline sql doesn't match: Old: %s, New: %s",
                        old_sql,
                        new_sql,
                    )
            except Exception as e:
                logger.exception(e)
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
