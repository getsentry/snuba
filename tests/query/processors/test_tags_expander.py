from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    binary_condition,
    get_first_level_and_conditions,
    in_condition,
)
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.snql.parser import parse_snql_query
from snuba.request.request_settings import HTTPRequestSettings


def test_tags_expander() -> None:
    query_body = """
    MATCH (events)
    SELECT count(platform) AS platforms, testF(platform, tags_value) AS top_platforms, f1(tags_key, column2) AS f1_alias, f2() AS f2_alias
    WHERE tags_key = 'tags_key'
    AND project_id = 1
    AND timestamp >= toDateTime('2020-01-01 12:00:00')
    AND timestamp < toDateTime('2020-01-02 12:00:00')
    HAVING tags_value IN tuple('tag')
    """

    events = get_dataset("events")
    query, _ = parse_snql_query(query_body, events)

    processor = TagsExpanderProcessor()
    request_settings = HTTPRequestSettings()
    processor.process_query(query, request_settings)

    assert query.get_selected_columns() == [
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

    condition = query.get_condition()
    assert condition is not None
    conds = get_first_level_and_conditions(condition)
    assert conds[0] == binary_condition(
        OPERATOR_TO_FUNCTION["="],
        FunctionCall("_snuba_tags_key", "arrayJoin", (Column(None, None, "tags.key"),)),
        Literal(None, "tags_key"),
    )

    assert query.get_having() == in_condition(
        FunctionCall(
            "_snuba_tags_value", "arrayJoin", (Column(None, None, "tags.value"),)
        ),
        [Literal(None, "tag")],
    )
