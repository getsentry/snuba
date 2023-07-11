from typing import Callable, Sequence

import sentry_sdk

from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta


def _execute_clickhouse_processors(
    processors: Callable[[ClickhouseQueryPlan], Sequence[ClickhouseQueryProcessor]],
    query_plan: ClickhouseQueryPlan,
    settings: QuerySettings,
) -> None:
    """
    Executes the Clickhouse query processors for the query. These
    are taken from the query plan.

    When executing a query plan there are two sequences of query processors.
    The first one is executed once per plan. The second is provided to the
    execution strategy to be executed at every database query.
    This function can be used in either case by customizing the sequence.
    """
    for clickhouse_processor in processors(query_plan):
        with sentry_sdk.start_span(
            description=type(clickhouse_processor).__name__, op="processor"
        ):
            clickhouse_processor.process_query(query_plan.query, settings)


def execute_plan_processors(
    query_plan: ClickhouseQueryPlan,
    settings: QuerySettings,
) -> None:
    """
    Executes the plan query processors but not the db ones (those
    that have to run for each db query).
    This is used when we rely on the query execution strategy to execute
    the db query processors.
    """
    _execute_clickhouse_processors(
        lambda plan: plan.plan_query_processors, query_plan, settings
    )


def execute_all_clickhouse_processors(
    query_plan: ClickhouseQueryPlan,
    settings: QuerySettings,
) -> None:
    """
    Executes all Clickhouse query processing including the plan processors
    and the db processors.
    This method can be useful when we want to fully process the query plan
    for a single storage but we do not want to execute the query through
    the the execution strategy.
    """
    _execute_clickhouse_processors(
        lambda plan: [*plan.plan_query_processors, *plan.db_query_processors],
        query_plan,
        settings,
    )


def execute_entity_processors(query: LogicalQuery, settings: QuerySettings) -> None:
    """
    Executes the entity query processors for the query. These are taken
    from the entity.
    """
    entity = get_entity(query.get_from_clause().key)

    for processor in entity.get_query_processors():
        with sentry_sdk.start_span(
            description=type(processor).__name__, op="processor"
        ):
            if settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "entity_processor", type(processor).__name__, query
                ):
                    processor.process_query(query, settings)
            else:
                processor.process_query(query, settings)
