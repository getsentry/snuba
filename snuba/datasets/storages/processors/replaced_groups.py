import logging
from dataclasses import replace
from typing import Optional

from snuba import environment, settings
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.errors_replacer import ReplacerState, get_projects_query_flags
from snuba.query.conditions import not_in_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper

logger = logging.getLogger(__name__)
metrics = MetricsWrapper(environment.metrics, "processors.replaced_groups")


class PostReplacementConsistencyEnforcer(QueryProcessor):
    """
    This processor tweaks the query to ensure that groups that have been manipulated
    by a replacer (like after a deletion) are excluded if they need to be.

    There is a period of time between the replacement executing its query and Clickhouse
    merging the rows to achieve consistency. During this period of time we either
    have to remove those rows manually or to run the query in FINAL mode.
    """

    def __init__(
        self, project_column: str, replacer_state_name: Optional[ReplacerState]
    ) -> None:
        self.__project_column = project_column
        # This is used to allow us to keep the replacement state in redis for multiple
        # replacers on multiple tables. replacer_state_name is part of the redis key.
        self.__replacer_state_name = replacer_state_name

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        if request_settings.get_turbo():
            return

        project_ids = get_object_ids_in_query_ast(query, self.__project_column)

        set_final = False
        if project_ids:
            final, exclude_group_ids, replacement_types = get_projects_query_flags(
                list(project_ids), self.__replacer_state_name,
            )
            tags = {replacement_type: "True" for replacement_type in replacement_types}
            tags["referrer"] = request_settings.referrer
            tags["parent_api"] = request_settings.get_parent_api()
            if final:
                tags["cause"] = "final_flag"
                metrics.increment(
                    "final", tags=tags,
                )
            if not final and exclude_group_ids:
                # If the number of groups to exclude exceeds our limit, the query
                # should just use final instead of the exclusion set.
                max_group_ids_exclude = get_config(
                    "max_group_ids_exclude", settings.REPLACER_MAX_GROUP_IDS_TO_EXCLUDE
                )
                assert isinstance(max_group_ids_exclude, int)
                if len(exclude_group_ids) > max_group_ids_exclude:
                    tags["cause"] = "max_groups"
                    metrics.increment(
                        "final", tags=tags,
                    )
                    set_final = True
                else:
                    query.add_condition_to_ast(
                        not_in_condition(
                            FunctionCall(
                                None, "assumeNotNull", (Column(None, None, "group_id"),)
                            ),
                            [Literal(None, p) for p in exclude_group_ids],
                        )
                    )
            else:
                set_final = final

        query.set_from_clause(replace(query.get_from_clause(), final=set_final))
