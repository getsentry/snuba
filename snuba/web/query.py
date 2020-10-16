import copy
import logging
from datetime import datetime
from functools import partial

import sentry_sdk
from snuba import environment
from snuba.clickhouse.astquery import AstSqlQuery
from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query.timeseries_extension import TimeSeriesExtensionProcessor
from snuba.querylog import record_query
from snuba.querylog.query_metadata import SnubaQueryMetadata
from snuba.reader import Reader
from snuba.request import Request
from snuba.request.request_settings import RequestSettings
from snuba.util import with_span
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryException, QueryResult
from snuba.web.db_query import raw_query

logger = logging.getLogger("snuba.query")

metrics = MetricsWrapper(environment.metrics, "api")


@with_span()
def parse_and_run_query(
    dataset: Dataset, request: Request, timer: Timer
) -> QueryResult:
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

    try:
        result = _run_query_pipeline(
            request=request, timer=timer, query_metadata=query_metadata
        )
        record_query(request_copy, timer, query_metadata)
    except QueryException as error:
        record_query(request_copy, timer, query_metadata)
        raise error

    return result


def _run_query_pipeline(
    request: Request, timer: Timer, query_metadata: SnubaQueryMetadata,
) -> QueryResult:
    """
    Runs the query processing and execution pipeline for a Snuba Query. This means it takes a Dataset
    and a Request and returns the results of the query.

    This process includes:
    - Applying dataset specific syntax extensions (QueryExtension)
    - Applying dataset query processors on the abstract Snuba query.
    - Using the dataset provided ClickhouseQueryPlanBuilder to build a ClickhouseQueryPlan. This step
      transforms the Snuba Query into the Storage Query (that is contextual to the storage/s).
      From this point on none should depend on the dataset.
    - Executing the plan specific query processors.
    - Providing the newly built Query, processors to be run for each DB query and a QueryRunner
      to the QueryExecutionStrategy to actually run the DB Query.
    """
    query_entity = request.query.get_entity()
    entity = get_entity(query_entity.key)

    # TODO: this will work perfectly with datasets that are not time series. Remove it.
    from_date, to_date = TimeSeriesExtensionProcessor.get_time_limit(
        request.extensions["timeseries"]
    )

    if (
        request.query.get_sample() is not None and request.query.get_sample() != 1.0
    ) and not request.settings.get_turbo():
        metrics.increment("sample_without_turbo", tags={"referrer": request.referrer})

    extensions = entity.get_extensions()

    for name, extension in extensions.items():
        with sentry_sdk.start_span(
            description=type(extension.get_processor()).__name__, op="extension"
        ):
            extension.get_processor().process_query(
                request.query, request.extensions[name], request.settings
            )

    # TODO: Fit this in a query processor. All query transformations should be driven by
    # datasets/storages and never hardcoded.
    if request.settings.get_turbo():
        request.query.set_final(False)

    for processor in entity.get_query_processors():
        with sentry_sdk.start_span(
            description=type(processor).__name__, op="processor"
        ):
            processor.process_query(request.query, request.settings)

    query_plan = entity.get_query_plan_builder().build_plan(request)
    # From this point on. The logical query should not be used anymore by anyone.
    # The Clickhouse Query is the one to be used to run the rest of the query pipeline.

    # TODO: Break the Query Plan execution out of this method. With the division
    # between plan specific processors and DB query specific processors and with
    # the soon to come ClickhouseCluster, there is more coupling between the
    # components of the query plan.

    for clickhouse_processor in query_plan.plan_processors:
        with sentry_sdk.start_span(
            description=type(clickhouse_processor).__name__, op="processor"
        ):
            clickhouse_processor.process_query(query_plan.query, request.settings)

    query_runner = partial(
        _format_storage_query_and_run,
        timer,
        query_metadata,
        from_date,
        to_date,
        request.referrer,
    )

    return query_plan.execution_strategy.execute(
        query_plan.query, request.settings, query_runner
    )


def _format_storage_query_and_run(
    timer: Timer,
    query_metadata: SnubaQueryMetadata,
    from_date: datetime,
    to_date: datetime,
    referrer: str,
    clickhouse_query: Query,
    request_settings: RequestSettings,
    reader: Reader[SqlQuery],
) -> QueryResult:
    """
    Formats the Storage Query and pass it to the DB specific code for execution.
    """

    # TODO: This function (well, it will be a wrapper of this function)
    # where we will transform the result according to the SelectedExpression
    # object in the query to ensure the fields in the QueryResult have
    # the same name the user expects.

    source = clickhouse_query.get_data_source().format_from()
    with sentry_sdk.start_span(description="create_query", op="db") as span:
        formatted_query = AstSqlQuery(clickhouse_query, request_settings)
        span.set_data("query", formatted_query.sql_data())
        metrics.increment("execute")

    timer.mark("prepare_query")

    stats = {
        "clickhouse_table": source,
        "final": clickhouse_query.get_final(),
        "referrer": referrer,
        "num_days": (to_date - from_date).days,
        "sample": clickhouse_query.get_sample(),
    }

    with sentry_sdk.start_span(
        description=formatted_query.format_sql(), op="db"
    ) as span:
        span.set_tag("table", source)

        return raw_query(
            clickhouse_query,
            request_settings,
            formatted_query,
            reader,
            timer,
            query_metadata,
            stats,
            span.trace_id,
        )
