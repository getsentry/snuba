import logging
import re

from typing import Mapping, Sequence

from snuba.datasets.schemas.join import JoinNode
from snuba.query.columns import all_referenced_columns
from snuba.query.query import Condition, Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


logger = logging.getLogger('snuba.query')


class DefaultConditionsProcessor(QueryProcessor):
    def __init__(
        self,
        basic_conditions: Sequence[Condition],
        aliased_conditions: Mapping[str, Sequence[Condition]],
    ) -> None:
        self.__basic_conditions = basic_conditions
        self.__aliased_conditions = aliased_conditions

    def process_query(self,
        query: Query,
        request_settings: RequestSettings,
    ) -> None:
        query.add_conditions(self.__basic_conditions)

        from_clause = query.get_data_source()
        if isinstance(from_clause, JoinNode):
            aliases = from_clause.get_tables().keys()
            for alias in aliases:
                conditions = self.__aliased_conditions.get(alias, [])
                query.add_conditions(conditions)
