import copy
import logging

from datetime import datetime

import sentry_sdk
from flask import request as http_request
from functools import partial

from snuba import environment, settings, state
from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.clickhouse.dictquery import DictClickhouseQuery
from snuba.datasets.dataset import Dataset
from snuba.datasets.factory import get_dataset_name
from snuba.query.timeseries import TimeSeriesExtensionProcessor
from snuba.request import Request
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.utils.metrics.timer import Timer
from snuba.web import RawQueryException, RawQueryResult
from snuba.web.db_query import raw_query
from snuba.web.query_metadata import SnubaQueryMetadata
from snuba.web.split import split_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")


def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> RawQueryResult:
    """
    Runs a Snuba Query, then records the metadata about each split query that was run.
    """
    request_copy = copy.deepcopy(request)
    query_metadata = SnubaQueryMetadata(
        request=request_copy,
        dataset=get_dataset_name(dataset),
        timer=timer,
        query_list=[],
    )

    with sentry_sdk.configure_scope() as scope:
        if scope.span:
            scope.span.set_tag("dataset", get_dataset_name(dataset))
            scope.span.set_tag("referrer", http_request.referrer)

    try:
        result = _run_query_pipeline(
            dataset=dataset, request=request, timer=timer, query_metadata=query_metadata
        )
        record_query(request_copy, timer, query_metadata)
    except RawQueryException as error:
        record_query(request_copy, timer, query_metadata)
        raise error

    return result


@split_query
def _run_query_pipeline(
    dataset: Dataset,
    request: Request,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
) -> RawQueryResult:
    """
    Runs the query processing and execution pipeline for a Snuba Query. This means it takes a Dataset
    and a Request and returns the results of the query.

    This process includes:
    - Applying dataset specific syntax extensions (QueryExtension)
    - Applying dataset query processors on the abstract Snuba query.
    - Using the dataset provided StorageQueryPlanBuilder to build a StorageQueryPlan. This step
      transforms the Snuba Query into the Storage Query (that is contextual to the storage/s).
      From this point on none should depend on the dataset.
    - Executing the storage specific query processors.
    - Providing the newly built Query and a QueryRunner to the QueryExecutionStrategy to actually
      run the DB Query.
    """

    # TODO: this will work perfectly with datasets that are not time series. Remove it.
    from_date, to_date = TimeSeriesExtensionProcessor.get_time_limit(
        request.extensions["timeseries"]
    )

    if (
        request.query.get_sample() is not None and request.query.get_sample() != 1.0
    ) and not request.settings.get_turbo():
        metrics.increment("sample_without_turbo", tags={"referrer": request.referrer})

    extensions = dataset.get_extensions()
    for name, extension in extensions.items():
        extension.get_processor().process_query(
            request.query, request.extensions[name], request.settings
        )

    # TODO: Fit this in a query processor. All query transformations should be driven by
    # datasets/storages and never hardcoded.
    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in dataset.get_query_processors():
        processor.process_query(request.query, request.settings)

    storage_query_plan = dataset.get_query_plan_builder().build_plan(request)

    # TODO: This below should be a storage specific query processor.
    relational_source = request.query.get_data_source()
    request.query.add_conditions(relational_source.get_mandatory_conditions())

    for processor in storage_query_plan.query_processors:
        processor.process_query(request.query, request.settings)

    query_runner = partial(
        _format_storage_query_and_run,
        dataset,
        timer,
        query_metadata,
        from_date,
        to_date,
    )

    return storage_query_plan.execution_strategy.execute(request, query_runner)


def _format_storage_query_and_run(
    # TODO: remove dependency on Dataset. This is only for formatting the legacy ClickhouseQuery
    # with the AST this won't be needed.
    dataset: Dataset,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    from_date: datetime,
    to_date: datetime,
    request: Request,
) -> RawQueryResult:
    """
    Formats the Storage Query and pass it to the DB specific code for execution.
    TODO: When we will have the AST in production and we will have the StorageQuery
    abstraction, this function is probably going to collapse and disappear.
    """

    source = request.query.get_data_source().format_from()
    with sentry_sdk.start_span(description="create_query", op="db"):
        # TODO: Move the performance logic and the pre_where generation into
        # ClickhouseQuery since they are Clickhouse specific
        query = DictClickhouseQuery(dataset, request.query, request.settings)
    timer.mark("prepare_query")

    stats = {
        "clickhouse_table": source,
        "final": request.query.get_final(),
        "referrer": request.referrer,
        "num_days": (to_date - from_date).days,
        "sample": request.query.get_sample(),
    }

    with sentry_sdk.start_span(description=query.format_sql(), op="db") as span:
        span.set_tag("table", source)
        try:
            span.set_tag(
                "ast_query",
                AstClickhouseQuery(request.query, request.settings).format_sql(),
            )
        except Exception:
            logger.warning("Failed to format ast query", exc_info=True)

        return raw_query(request, query, timer, query_metadata, stats, span.trace_id)


def record_query(
    request: Request, timer: Timer, query_metadata: SnubaQueryMetadata
) -> None:
    if settings.RECORD_QUERIES:
        # Send to redis
        # We convert this to a dict before passing it to state in order to avoid a
        # circular dependency, where state would depend on the higher level
        # QueryMetadata class
        state.record_query(query_metadata.to_dict())

        final = str(request.query.get_final())
        referrer = request.referrer or "none"
        timer.send_metrics_to(
            metrics,
            tags={
                "status": query_metadata.status,
                "referrer": referrer,
                "final": final,
            },
            mark_tags={"final": final},
        )
