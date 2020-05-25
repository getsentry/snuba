from dataclasses import replace

from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class TagsExpanderProcessor(QueryProcessor):
    """
    Transforms the special syntax we provide to expand the tags column into a call
    to arrayJoin so that, after this query processor, tags_key and tags_value special
    columns are not treated as a special case but they are valid expressions on a
    valid column name.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_expression(exp: Expression) -> Expression:
            # This is intentionally not configurable in order to discourage creating
            # a special syntax for expressions that should be function calls.
            if isinstance(exp, Column) and exp.column_name in (
                "tags_key",
                "tags_value",
            ):
                name_split = exp.column_name.split("_")
                return FunctionCall(
                    exp.alias or exp.column_name,
                    "arrayJoin",
                    (
                        replace(
                            exp,
                            alias=None,
                            column_name=name_split[0],
                            path=(name_split[1],),
                        ),
                    ),
                )
            return exp

        query.transform_expressions(transform_expression)
