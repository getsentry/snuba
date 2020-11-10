from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import OPERATOR_TO_FUNCTION, binary_condition, in_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_tags_expander() -> None:
    query_body = {
        "selected_columns": [
            ["f1", ["tags_key", "column2"], "f1_alias"],
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
    query = parse_query(query_body, events)

    processor = TagsExpanderProcessor()
    request_settings = HTTPRequestSettings()
    processor.process_query(query, request_settings)

    assert query.get_selected_columns_from_ast() == [
        SelectedExpression(
            "platforms",
            FunctionCall(
                "_snuba_platforms",
                "count",
                (Column("_snuba_platform", None, "platform"),),
            ),
        ),
        SelectedExpression(
            "top_platforms",
            FunctionCall(
                "_snuba_top_platforms",
                "testF",
                (
                    Column("_snuba_platform", None, "platform"),
                    FunctionCall(
                        "_snuba_tags_value",
                        "arrayJoin",
                        (Column(None, None, "tags.value"),),
                    ),
                ),
            ),
        ),
        SelectedExpression(
            "f1_alias",
            FunctionCall(
                "_snuba_f1_alias",
                "f1",
                (
                    FunctionCall(
                        "_snuba_tags_key",
                        "arrayJoin",
                        (Column(None, None, "tags.key"),),
                    ),
                    Column("_snuba_column2", None, "column2"),
                ),
            ),
        ),
        SelectedExpression("f2_alias", FunctionCall("_snuba_f2_alias", "f2", tuple())),
    ]

    assert query.get_condition_from_ast() == binary_condition(
        None,
        OPERATOR_TO_FUNCTION["="],
        FunctionCall("_snuba_tags_key", "arrayJoin", (Column(None, None, "tags.key"),)),
        Literal(None, "tags_key"),
    )

    assert query.get_having_from_ast() == in_condition(
        None,
        FunctionCall(
            "_snuba_tags_value", "arrayJoin", (Column(None, None, "tags.value"),)
        ),
        [Literal(None, "tag")],
    )
