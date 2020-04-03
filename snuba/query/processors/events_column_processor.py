from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class EventsColumnProcessor(QueryProcessor):
    """
    Strip any dashes out of the event ID to match what is stored internally.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name == "group_id":
                    return FunctionCall(
                        exp.alias,
                        "nullIf",
                        (Column(None, exp.column_name, None), Literal(None, 0),),
                    )
                elif exp.column_name == "message":
                    # Because of the rename from message->search_message without backfill,
                    # records will have one or the other of these fields.
                    # TODO this can be removed once all data has search_message filled in.
                    return FunctionCall(
                        exp.alias,
                        "coalesce",
                        (
                            Column(None, exp.column_name, None),
                            Column(None, "search_message", None),
                        ),
                    )

                # This assumes that the tag processor is converting from `contexts[device.simulator]`
                # before this is called, otherwise trouble will ensue.
                #
                # This conversion must not be ported to the errors dataset. We should
                # not support promoting tags/contexts with boolean values. There is
                # no way to convert them back consistently to the value provided by
                # the client when the event is ingested, in all ways to access
                # tags/contexts. Once the errors dataset is in use, we will not have
                # boolean promoted tags/contexts so this constraint will be easy to enforce.
                boolean_contexts = {
                    "device_simulator",
                    "device_online",
                    "device_charging",
                }
                if exp.column_name in boolean_contexts:
                    return FunctionCall(
                        exp.alias,
                        "multiIf",
                        (
                            binary_condition(
                                None,
                                ConditionFunctions.EQ,
                                Column(None, exp.column_name, None),
                                Literal(None, ""),
                            ),
                            Literal(None, ""),
                            binary_condition(
                                None,
                                ConditionFunctions.IN,
                                Column(None, exp.column_name, None),
                                literals_tuple(
                                    None, [Literal(None, "1"), Literal(None, "True")]
                                ),
                            ),
                            Literal(None, "True"),
                            Literal(None, "False"),
                        ),
                    )

            return exp

        query.transform_expressions(process_column)
