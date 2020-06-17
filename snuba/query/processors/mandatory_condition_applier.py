from snuba.query.logical import Query
from snuba.clickhouse.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings

from snuba.request import Request
from snuba.datasets.dataset import Dataset
from snuba.query.conditions import combine_and_conditions


class MandatoryConditionApplier(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:

        query_plan = Dataset.get_query_plan_builder().build_plan(Request)

        relational_source = query_plan.query.get_data_source()
        mandatory_conditions = relational_source.get_mandatory_conditions()
        query_plan.query.add_conditions([c.legacy for c in mandatory_conditions])

        if len(mandatory_conditions) > 0:
            query_plan.query.add_condition_to_ast(
                combine_and_conditions([c.ast for c in mandatory_conditions])
            )
