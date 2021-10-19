from snuba.clickhouse.formatter.nodes import (
    FormattedQuery,
    FormattedSubQuery,
    PaddingNode,
    SequenceNode,
    StringNode,
)


def test_composite_query() -> None:
    query = FormattedQuery(
        [
            StringNode("SELECT avg(a)"),
            PaddingNode(
                "FROM",
                FormattedSubQuery(
                    [
                        StringNode("SELECT t_a.a, t_b.b"),
                        PaddingNode(
                            "FROM",
                            SequenceNode(
                                [
                                    PaddingNode(
                                        None,
                                        FormattedSubQuery(
                                            [
                                                StringNode("SELECT a, b"),
                                                StringNode("FROM somewhere"),
                                            ]
                                        ),
                                        "t_a",
                                    ),
                                    StringNode("INNER SEMI JOIN"),
                                    PaddingNode(
                                        None,
                                        FormattedSubQuery(
                                            [
                                                StringNode("SELECT a, b"),
                                                StringNode("FROM somewhere_else"),
                                            ]
                                        ),
                                        "t_b",
                                    ),
                                    StringNode("ON t_a.a = t_b.b"),
                                ],
                            ),
                        ),
                    ],
                ),
            ),
            StringNode("WHERE something something"),
        ],
    )

    assert query.get_sql(format="JSON") == (
        "SELECT avg(a) FROM "
        "(SELECT t_a.a, t_b.b FROM "
        "(SELECT a, b FROM somewhere) t_a "
        "INNER SEMI JOIN "
        "(SELECT a, b FROM somewhere_else) t_b "
        "ON t_a.a = t_b.b) "
        "WHERE something something "
        "FORMAT JSON"
    )

    assert query.structured() == [
        "SELECT avg(a)",
        [
            "FROM",
            [
                "SELECT t_a.a, t_b.b",
                [
                    "FROM",
                    [
                        [["SELECT a, b", "FROM somewhere"], "t_a"],
                        "INNER SEMI JOIN",
                        [["SELECT a, b", "FROM somewhere_else"], "t_b"],
                        "ON t_a.a = t_b.b",
                    ],
                ],
            ],
        ],
        "WHERE something something",
    ]
