from snuba.datasets.factory import get_dataset
from snuba.query.conditions import OPERATOR_TO_FUNCTION, binary_condition, in_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import build_query
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_tags_expander() -> None:
    query_body = {
        "selected_columns": [
            ["f1", ["tags_key", "event_id"], "f1_alias"],
            ["f2", [], "f2_alias"],
        ],
        "aggregations": [
            ["count", "platform", "platforms"],
            ["testF", ["platform", "tags_value"], "top_platforms"],
        ],
        "conditions": [["tags_key", "=", "tags_key"]],
        "having": [["tags_value", "IN", ["tag"]]],
    }

    events = get_dataset("events")
    query = build_query(query_body, events)

    processor = TagsExpanderProcessor()
    request_settings = HTTPRequestSettings()
    processor.process_query(query, request_settings)

    assert query.get_selected_columns_from_ast() == [
        FunctionCall("platforms", "count", (Column(None, None, "platform"),)),
        FunctionCall(
            "top_platforms",
            "testF",
            (
                Column(None, None, "platform"),
                FunctionCall(
                    "tags_value", "arrayJoin", (Column(None, None, "tags", ("value",)),)
                ),
            ),
        ),
        FunctionCall(
            "f1_alias",
            "f1",
            (
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags", ("key",)),)
                ),
                Column(None, None, "event_id"),
            ),
        ),
        FunctionCall("f2_alias", "f2", tuple()),
    ]

    assert query.get_condition_from_ast() == binary_condition(
        None,
        OPERATOR_TO_FUNCTION["="],
        FunctionCall("tags_key", "arrayJoin", (Column(None, None, "tags", ("key",)),)),
        Literal(None, "tags_key"),
    )

    assert query.get_having_from_ast() == in_condition(
        None,
        FunctionCall(
            "tags_value", "arrayJoin", (Column(None, None, "tags", ("value",)),)
        ),
        [Literal(None, "tag")],
    )
