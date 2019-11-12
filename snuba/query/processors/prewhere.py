from typing import Sequence

from snuba import settings, util
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings


class PreWhereProcessor(QueryProcessor):
    """
    Moves top level conditions into the pre-where clause
    according to the list of candidates provided by the query data source.

    In order for a condition to become a pre-where condition it has
    to be:
    - a single top-level condition (not in an OR statement)
    - any of its referenced columns must be in the list provided to the
      constructor.
    """

    def process_query(self,
        query: Query,
        request_settings: RequestSettings,
    ) -> None:
        pre_where_keys = query.get_data_source().get_prewhere_candiates()
        if not pre_where_keys:
            return
        prewhere_conditions: Sequence[Condition] = []
        # Add any condition to PREWHERE if:
        # - It is a single top-level condition (not OR-nested), and
        # - Any of its referenced columns are in pre_where_keys
        conditions = query.get_conditions()
        if not conditions:
            return
        prewhere_candidates = [
            (util.columns_in_expr(cond[0]), cond)
            for cond in conditions if util.is_condition(cond) and
            any(col in pre_where_keys for col in util.columns_in_expr(cond[0]))
        ]
        # Use the condition that has the highest priority (based on the
        # position of its columns in the prewhere keys list)
        prewhere_candidates = sorted([
            (min(pre_where_keys.index(col) for col in cols if col in pre_where_keys), cond)
            for cols, cond in prewhere_candidates
        ], key=lambda priority_and_col: priority_and_col[0])
        if prewhere_candidates:
            prewhere_conditions = [cond for _, cond in prewhere_candidates][:settings.MAX_PREWHERE_CONDITIONS]
            query.set_conditions(
                list(filter(lambda cond: cond not in prewhere_conditions, conditions))
            )
        query.set_prewhere(prewhere_conditions)
