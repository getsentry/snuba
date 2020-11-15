from typing import Callable, Sequence, Tuple

import sentry_sdk
from snuba.clickhouse.processors import QueryProcessor
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import ClickhouseQueryPlan
from snuba.pipeline import Segment
from snuba.pipeline.query_pipeline import EntityProcessingPayload
from snuba.request.request_settings import RequestSettings

ClickhouseProcessingPayload = Tuple[ClickhouseQueryPlan, RequestSettings]


class ClickhouseProcessorsExecutor(Segment[ClickhouseProcessingPayload, None]):
    """
    Executes the Clickhouse query processors for the query. These
    are taken from the query plan.

    This segment does not return anything because we still work with
    the assumption that the query is transformed in place.

    When executing a query plan there are two sequences of query processors.
    The first one is executed once per plan. The second is provided to the
    execution strategy to be executed at every database query.
    This segment can be used in either case by customizing the sequence.
    """

    def __init__(
        self, processors: Callable[[ClickhouseQueryPlan], Sequence[QueryProcessor]]
    ) -> None:
        # This function builds the sequence of processors to be executed
        # from the ClickhouseQueryPlan.
        self.__processors = processors

    def execute(self, processing_payload: ClickhouseProcessingPayload) -> None:
        query_plan, settings = processing_payload
        for clickhouse_processor in self.__processors(query_plan):
            with sentry_sdk.start_span(
                description=type(clickhouse_processor).__name__, op="processor"
            ):
                clickhouse_processor.process_query(query_plan.query, settings)


class EntityProcessorsExecutor(Segment[EntityProcessingPayload, None]):
    """
    Executes the entity query processors for the query. These are taken
    from the entity.

    This segment does not return anything because we still work with
    the assumption that the query is transformed in place.
    """

    def execute(self, processing_payload: EntityProcessingPayload) -> None:
        query, settings = processing_payload
        entity = get_entity(query.get_from_clause().key)

        for processor in entity.get_query_processors():
            with sentry_sdk.start_span(
                description=type(processor).__name__, op="processor"
            ):
                processor.process_query(query, settings)
