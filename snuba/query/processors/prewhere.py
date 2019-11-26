from typing import Optional, Sequence

from snuba import settings, util
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.types import Condition
from snuba.request.request_settings import RequestSettings


class PrewhereProcessor(QueryProcessor):
    """
    Moves top level conditions into the pre-where clause
    according to the list of candidates provided by the query data source.

    In order for a condition to become a pre-where condition it has
    to be:
    - a single top-level condition (not in an OR statement)
    - any of its referenced columns must be in the list provided to the
      constructor.
    """

    def __init__(self, max_prewhere_conditions: Optional[int] = None) -> None:
        self.__max_prewhere_conditions: int = max_prewhere_conditions if max_prewhere_conditions is not None else settings.MAX_PREWHERE_CONDITIONS

    def process_query(self, query: Query, request_settings: RequestSettings,) -> None:
        prewhere_keys = query.get_data_source().get_prewhere_candidates()
        if not prewhere_keys:
            return
        prewhere_conditions: Sequence[Condition] = []
        # Add any condition to PREWHERE if:
        # - It is a single top-level condition (not OR-nested), and
        # - Any of its referenced columns are in prewhere_keys
        conditions = query.get_conditions()
        if not conditions:
            return
        prewhere_candidates = [
            (util.columns_in_expr(cond[0]), cond)
            for cond in conditions
            if util.is_condition(cond)
            and any(col in prewhere_keys for col in util.columns_in_expr(cond[0]))
        ]
        # Use the condition that has the highest priority (based on the
        # position of its columns in the prewhere keys list)
        prewhere_candidates = sorted(
            [
                (
                    min(
                        prewhere_keys.index(col) for col in cols if col in prewhere_keys
                    ),
                    cond,
                )
                for cols, cond in prewhere_candidates
            ],
            key=lambda priority_and_col: priority_and_col[0],
        )
        if prewhere_candidates:
            prewhere_conditions = [cond for _, cond in prewhere_candidates][
                : self.__max_prewhere_conditions
            ]
            query.set_conditions(
                list(filter(lambda cond: cond not in prewhere_conditions, conditions))
            )
        query.set_prewhere(prewhere_conditions)
