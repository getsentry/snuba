import copy
import logging

from datetime import datetime

import sentry_sdk
from flask import request as http_request
from functools import partial

from snuba import environment, settings, state
from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.clickhouse.query import DictClickhouseQuery
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
    Runs a query, then records the metadata about each split query that was run.
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
            scope.span.set_tag("dataset", type(dataset).__name__)
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
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    from_date: datetime,
    to_date: datetime,
    request: Request,
) -> RawQueryResult:
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

    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in dataset.get_query_processors():
        processor.process_query(request.query, request.settings)

    storage_query_plan = dataset.get_query_plan_builder().build_plan(request)

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
    dataset: Dataset,
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    from_date: datetime,
    to_date: datetime,
    request: Request,
) -> RawQueryResult:
    # TODO: This below should be a query processor.
    relational_source = request.query.get_data_source()
    request.query.add_conditions(relational_source.get_mandatory_conditions())

    source = relational_source.format_from()
    with sentry_sdk.start_span(description="create_query", op="db"):
        # TODO: Move the performance logic and the pre_where generation into
        # ClickhouseQuery since they are Clickhouse specific
        query = DictClickhouseQuery(dataset, request.query, request.settings)
    timer.mark("prepare_query")

    num_days = (to_date - from_date).days
    stats = {
        "clickhouse_table": source,
        "final": request.query.get_final(),
        "referrer": request.referrer,
        "num_days": num_days,
        "sample": request.query.get_sample(),
    }

    with sentry_sdk.start_span(description=query.format_sql(), op="db") as span:
        span.set_tag("dataset", type(dataset).__name__)
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
