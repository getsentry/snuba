from datetime import datetime
from typing import Union

import pytest

from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.processors.tagsmap import NestedFieldConditionOptimizer
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings


def build_cond(
    col: str, operator: str, operand: Union[str, int, datetime]
) -> Expression:
    return binary_condition(
        None,
        OPERATOR_TO_FUNCTION[operator],
        Column(None, None, col),
        Literal(None, operand),
    )


test_data = [
    pytest.param(
        {
            "conditions": [
                ["d", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [["d", "=", "1"], ["c", "=", "3"], ["start_ts", ">", "2019-12-18T06:35:17"]],
        combine_and_conditions(
            [
                build_cond("d", "=", "1"),
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
            ]
        ),
        id="no tags",
    ),  # No tags
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%"),
            ]
        ),
        id="simple flattened tag",
    ),  # One simple tag condition
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["finish_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["finish_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("finish_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%"),
            ]
        ),
        id="simple flattened tag other timestamp",
    ),  # One simple tag condition, different timestamp
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-01-01T06:35:17"],
            ]
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-01-01T06:35:17"],
        ],
        combine_and_conditions(
            [
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    build_mapping_expr(
                        "tags[test.tag]", None, "tags", Literal(None, "test.tag")
                    ),
                    Literal(None, "1"),
                ),
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 1, 1, 6, 35, 17)),
            ]
        ),
        id="start_ts too old",
    ),  # Query start from before the existence of tagsmap
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-01-01T06:35:17"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-01-01T06:35:17"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 1, 1, 6, 35, 17)),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%"),
            ]
        ),
        id="two timestamps in the query",
    ),  # Two start conditions: apply
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond(
                    "tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"
                ),
            ]
        ),
        id="multiple tag conditions",
    ),  # Multiple tags in the same merge
    pytest.param(
        {
            "conditions": [
                ["tags[test2.tag]", "=", "2"],
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["tags[test3.tag]", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond(
                    "tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"
                ),
            ]
        ),
        id="enforce condition sorting",
    ),  # Multiple tags in the same merge and properly sorted
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "!=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "!=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test2.tag=2|%"],
            ["tags_map", "NOT LIKE", "%|test.tag=1|%"],
            ["tags_map", "NOT LIKE", "%|test3.tag=3|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test2.tag=2|%"),
                build_cond("tags_map", "NOT LIKE", "%|test.tag=1|%"),
                build_cond("tags_map", "NOT LIKE", "%|test3.tag=3|%"),
            ]
        ),
        id="negative condition",
    ),  # Negative conditions mixed with positive ones
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["func", ["tags[test2.tag]"]], "=", "2"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            [["func", ["tags[test2.tag]"]], "=", "2"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    FunctionCall(
                        None,
                        "func",
                        (
                            build_mapping_expr(
                                "tags[test2.tag]",
                                None,
                                "tags",
                                Literal(None, "test2.tag"),
                            ),
                        ),
                    ),
                    Literal(None, "2"),
                ),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%"),
            ]
        ),
        id="nested conditions",
    ),  # Nested condition. Only the external one is converted
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["ifNull", ["tags[test2.tag]", ""]], "=", "2"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%"),
            ]
        ),
        id="ifNull function",
    ),  # Nested conditions in ifNull. This is converted.
    pytest.param(
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["contexts[test.context]", "=", "1"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["contexts[test.context]", "=", "1"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
        combine_and_conditions(
            [
                build_cond("c", "=", "3"),
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    build_mapping_expr(
                        "contexts[test.context]",
                        None,
                        "contexts",
                        Literal(None, "test.context"),
                    ),
                    Literal(None, "1"),
                ),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
                build_cond("tags_map", "LIKE", "%|test.tag=1|%"),
            ]
        ),
        id="contexts and tags",
    ),  # Both contexts and tags are present
    pytest.param(
        {
            "conditions": [
                [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
                [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
            [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
        combine_and_conditions(
            [
                combine_or_conditions(
                    [
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            build_mapping_expr(
                                "tags[test.tag]",
                                None,
                                "tags",
                                Literal(None, "test.tag"),
                            ),
                            Literal(None, "1"),
                        ),
                        build_cond("c", "=", "3"),
                    ]
                ),
                combine_or_conditions(
                    [
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            build_mapping_expr(
                                "tags[test2.tag]",
                                None,
                                "tags",
                                Literal(None, "test2.tag"),
                            ),
                            Literal(None, "2"),
                        ),
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            build_mapping_expr(
                                "tags[test3.tag]",
                                None,
                                "tags",
                                Literal(None, "test3.tag"),
                            ),
                            Literal(None, "3"),
                        ),
                    ]
                ),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
            ]
        ),
        id="OR condition",
    ),  # Nested conditions, ignored.
    pytest.param(
        {
            "groupby": ["tags[another_tag]"],
            "having": [["tags[yet_another_tag]", "=", "1"]],
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
        combine_and_conditions(
            [
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    build_mapping_expr(
                        "tags[test.tag]", None, "tags", Literal(None, "test.tag")
                    ),
                    Literal(None, "1"),
                ),
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
            ]
        ),
        id="tags outside conditions",
    ),  # Skip using flattened tags if the query requires tags unpacking anyway
    pytest.param(
        {
            "orderby": ["tags[test.tag]"],
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
        combine_and_conditions(
            [
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    build_mapping_expr(
                        "tags[test.tag]", None, "tags", Literal(None, "test.tag")
                    ),
                    Literal(None, "1"),
                ),
                build_cond("c", "=", "3"),
                build_cond("start_ts", ">", datetime(2019, 12, 18, 6, 35, 17)),
            ]
        ),
        id="tags in groupby",
    ),  # Skip using flattened tags if the query requires tags unpacking anyway
]


@pytest.mark.parametrize(
    "query_body, expected_condition, ast_expected_condition", test_data
)
def test_nested_optimizer(
    query_body, expected_condition, ast_expected_condition
) -> None:
    transactions = get_dataset("transactions")
    query = parse_query(query_body, transactions)
    request_settings = HTTPRequestSettings()

    query_plan = transactions.get_query_plan_builder().build_plan(
        Request("", query, request_settings, {}, "")
    )
    processor = NestedFieldConditionOptimizer(
        nested_col="tags",
        flattened_col="tags_map",
        timestamp_cols={"start_ts", "finish_ts"},
        beginning_of_time=datetime(2019, 12, 11, 0, 0, 0),
    )
    clickhouse_query = query_plan.query
    processor.process_query(clickhouse_query, request_settings)

    assert clickhouse_query.get_conditions() == expected_condition
    assert clickhouse_query.get_condition_from_ast() == ast_expected_condition
