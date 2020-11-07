from snuba.clickhouse.query_formatter import (
    FormattedQuery,
    FormattedSubQuery,
    OrderedNestedWithPrefix,
    StringNode,
)


def test_composite_query() -> None:
    query = FormattedQuery(
        [
            ("select", StringNode("SELECT avg(a)")),
            (
                "from",
                FormattedSubQuery(
                    [
                        ("select", StringNode("SELECT t_a.a, t_b.b")),
                        (
                            "from",
                            OrderedNestedWithPrefix(
                                [
                                    (
                                        "t_a",
                                        FormattedSubQuery(
                                            [
                                                ("select", StringNode("SELECT a, b")),
                                                ("from", StringNode("FROM somewhere")),
                                            ]
                                        ),
                                    ),
                                    ("t_a_alias", StringNode("t_a")),
                                    ("join", StringNode("INNER SEMI JOIN")),
                                    (
                                        "t_b",
                                        FormattedSubQuery(
                                            [
                                                ("select", StringNode("SELECT a, b")),
                                                (
                                                    "from",
                                                    StringNode("FROM somewhere_else"),
                                                ),
                                            ]
                                        ),
                                    ),
                                    ("t_b_alias", StringNode("t_b")),
                                    ("join_cond", StringNode("ON t_a.a = t_b.b")),
                                ],
                                "FROM ",
                            ),
                        ),
                    ],
                    "FROM ",
                ),
            ),
            ("where", StringNode("WHERE something something")),
        ]
    )

    assert query.for_mapping() == {
        "select": "SELECT avg(a)",
        "from": {
            "select": "SELECT t_a.a, t_b.b",
            "from": {
                "t_a": {"select": "SELECT a, b", "from": "FROM somewhere"},
                "t_a_alias": "t_a",
                "join": "INNER SEMI JOIN",
                "t_b": {"select": "SELECT a, b", "from": "FROM somewhere_else"},
                "t_b_alias": "t_b",
                "join_cond": "ON t_a.a = t_b.b",
            },
        },
        "where": "WHERE something something",
    }

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
