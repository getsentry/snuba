from copy import deepcopy
from datetime import date, datetime
import pytest

from tests.base import BaseTest

from snuba.clickhouse.escaping import escape_identifier, escape_alias
from snuba.datasets.factory import get_dataset
from snuba import state
from snuba.query.columns import (
    column_expr,
    complex_column_expr,
    conditions_expr,
)
from snuba.query.logical import Query
from snuba.query.parsing import ParsingContext
from snuba.util import (
    escape_literal,
    tuplify,
)

DATASETS = [
    get_dataset("events"),
]


class TestUtil(BaseTest):
    def test_escape(self):
        assert escape_literal(r"'") == r"'\''"
        assert escape_literal(r"\'") == r"'\\\''"
        assert escape_literal(date(2001, 1, 1)) == "toDate('2001-01-01', 'Universal')"
        assert (
            escape_literal(datetime(2001, 1, 1, 1, 1, 1))
            == "toDateTime('2001-01-01T01:01:01', 'Universal')"
        )
        assert (
            escape_literal([1, "a", date(2001, 1, 1)])
            == "(1, 'a', toDate('2001-01-01', 'Universal'))"
        )

    def test_escape_identifier(self):
        assert escape_identifier(None) is None
        assert escape_identifier("") == ""
        assert escape_identifier("foo") == "foo"
        assert escape_identifier("foo.bar") == "foo.bar"
        assert escape_identifier("foo:bar") == "`foo:bar`"

        # Even though backtick characters in columns should be
        # disallowed by the query schema, make sure we dont allow
        # injection anyway.
        assert escape_identifier("`") == r"`\``"
        assert escape_identifier("production`; --") == r"`production\`; --`"

    def test_escape_alias(self):
        assert escape_alias(None) is None
        assert escape_alias("") == ""
        assert escape_alias("foo") == "foo"
        assert escape_alias("foo.bar") == "`foo.bar`"
        assert escape_alias("foo:bar") == "`foo:bar`"

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_conditions_expr(self, dataset):
        state.set_config("use_escape_alias", 1)
        conditions = [["a", "=", 1]]
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "a = 1"
        )

        conditions = []
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == ""
        )

        conditions = [[[]], []]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == ""
        )

        conditions = [[["a", "=", 1]]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "a = 1"
        )

        conditions = [["a", "=", 1], ["b", "=", 2]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "a = 1 AND b = 2"
        )

        conditions = [[["a", "=", 1], ["b", "=", 2]]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "(a = 1 OR b = 2)"
        )

        conditions = [[["a", "=", 1], ["b", "=", 2]], ["c", "=", 3]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "(a = 1 OR b = 2) AND c = 3"
        )

        conditions = [[["a", "=", 1], ["b", "=", 2]], [["c", "=", 3], ["d", "=", 4]]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "(a = 1 OR b = 2) AND (c = 3 OR d = 4)"
        )

        # Malformed condition input
        conditions = [[["a", "=", 1], []]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "a = 1"
        )

        # Test column expansion
        conditions = [[["tags[foo]", "=", 1], ["b", "=", 2]]]
        expanded = column_expr(
            dataset, "tags[foo]", Query({}, source), ParsingContext()
        )
        assert conditions_expr(
            dataset, conditions, Query({}, source), ParsingContext()
        ) == "({} = 1 OR b = 2)".format(expanded)

        # Test using alias if column has already been expanded in SELECT clause
        reuse_query = Query({}, source)
        parsing_context = ParsingContext()
        conditions = [[["tags[foo]", "=", 1], ["b", "=", 2]]]
        column_expr(
            dataset, "tags[foo]", reuse_query, parsing_context
        )  # Expand it once so the next time is aliased
        assert (
            conditions_expr(dataset, conditions, reuse_query, parsing_context)
            == "(`tags[foo]` = 1 OR b = 2)"
        )

        # Test special output format of LIKE
        conditions = [["primary_hash", "LIKE", "%foo%"]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "primary_hash LIKE '%foo%'"
        )

        conditions = tuplify(
            [[["notEmpty", ["arrayElement", ["exception_stacks.type", 1]]], "=", 1]]
        )
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "notEmpty(arrayElement((exception_stacks.type AS `exception_stacks.type`), 1)) = 1"
        )

        conditions = tuplify([[["notEmpty", ["tags[sentry:user]"]], "=", 1]])
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 1"
        )

        conditions = tuplify([[["notEmpty", ["tags_key"]], "=", 1]])
        assert (
            conditions_expr(
                dataset,
                conditions,
                Query({"conditions": [[["notEmpty", ["tags_key"]], "=", 1]]}, source),
                ParsingContext(),
            )
            == "notEmpty((arrayJoin(tags.key) AS tags_key)) = 1"
        )

        conditions = tuplify(
            [
                [
                    [["notEmpty", ["tags[sentry:environment]"]], "=", "dev"],
                    [["notEmpty", ["tags[sentry:environment]"]], "=", "prod"],
                ],
                [
                    [["notEmpty", ["tags[sentry:user]"]], "=", "joe"],
                    [["notEmpty", ["tags[sentry:user]"]], "=", "bob"],
                ],
            ]
        )
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == """(notEmpty((arrayElement(tags.value, indexOf(tags.key, 'sentry:environment')) AS `tags[sentry:environment]`)) = 'dev' OR notEmpty(`tags[sentry:environment]`) = 'prod') AND (notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 'joe' OR notEmpty(`tags[sentry:user]`) = 'bob')"""
        )

        # Test scalar condition on array column is expanded as an iterator.
        conditions = [["exception_frames.filename", "LIKE", "%foo%"]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "arrayExists(x -> assumeNotNull(x LIKE '%foo%'), (exception_frames.filename AS `exception_frames.filename`))"
        )

        # Test negative scalar condition on array column is expanded as an all() type iterator.
        conditions = [["exception_frames.filename", "NOT LIKE", "%foo%"]]
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "arrayAll(x -> assumeNotNull(x NOT LIKE '%foo%'), (exception_frames.filename AS `exception_frames.filename`))"
        )

        # Test that a duplicate IN condition is deduplicated even if
        # the lists are in different orders.[
        conditions = tuplify(
            [["platform", "IN", ["a", "b", "c"]], ["platform", "IN", ["c", "b", "a"]]]
        )
        assert (
            conditions_expr(dataset, conditions, Query({}, source), ParsingContext())
            == "platform IN ('a', 'b', 'c')"
        )

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_duplicate_expression_alias(self, dataset):
        body = {
            "aggregations": [
                ["top3", "logger", "dupe_alias"],
                ["uniq", "environment", "dupe_alias"],
            ]
        }
        parsing_context = ParsingContext()
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        # In the case where 2 different expressions are aliased
        # to the same thing, one ends up overwriting the other.
        # This may not be ideal as it may mask bugs in query conditions
        exprs = [
            column_expr(dataset, col, Query(body, source), parsing_context, alias, agg)
            for (agg, col, alias) in body["aggregations"]
        ]
        assert exprs == ["(topK(3)(logger) AS dupe_alias)", "dupe_alias"]

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_nested_aggregate_legacy_format(self, dataset):
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        priority = [
            "toUInt64(plus(multiply(log(times_seen), 600), last_seen))",
            "",
            "priority",
        ]
        assert (
            column_expr(
                dataset,
                "",
                Query({"aggregations": [priority]}, source),
                ParsingContext(),
                priority[2],
                priority[0],
            )
            == "(toUInt64(plus(multiply(log(times_seen), 600), last_seen)) AS priority)"
        )

        top_k = ["topK(3)", "logger", "top_3"]
        assert (
            column_expr(
                dataset,
                top_k[1],
                Query({"aggregations": [top_k]}, source),
                ParsingContext(),
                top_k[2],
                top_k[0],
            )
            == "(topK(3)(logger) AS top_3)"
        )

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_complex_conditions_expr(self, dataset):
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        query = Query({}, source)

        assert (
            complex_column_expr(
                dataset, tuplify(["count", []]), deepcopy(query), ParsingContext()
            )
            == "count()"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["notEmpty", ["foo"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "notEmpty(foo)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["notEmpty", ["arrayElement", ["foo", 1]]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "notEmpty(arrayElement(foo, 1))"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["foo", ["bar", ["qux"], "baz"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "foo(bar(qux), baz)"
        )
        assert (
            complex_column_expr(
                dataset, tuplify(["foo", [], "a"]), deepcopy(query), ParsingContext()
            )
            == "(foo() AS a)"
        )
        state.set_config("format_clickhouse_arrays", 1)
        assert (
            complex_column_expr(
                dataset,
                tuplify(["array", [1, 2, 3], "a"]),
                deepcopy(query),
                ParsingContext(),
            )
            == "([1, 2, 3] AS a)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["foo", ["b", "c"], "d"]),
                deepcopy(query),
                ParsingContext(),
            )
            == "(foo(b, c) AS d)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["foo", ["b", "c", ["d"]]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "foo(b, c(d))"
        )

        assert (
            complex_column_expr(
                dataset,
                tuplify(["top3", ["project_id"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "topK(3)(project_id)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["top10", ["project_id"], "baz"]),
                deepcopy(query),
                ParsingContext(),
            )
            == "(topK(10)(project_id) AS baz)"
        )

        assert (
            complex_column_expr(
                dataset,
                tuplify(["emptyIfNull", ["project_id"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "ifNull(project_id, '')"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["emptyIfNull", ["project_id"], "foo"]),
                deepcopy(query),
                ParsingContext(),
            )
            == "(ifNull(project_id, '') AS foo)"
        )

        assert (
            complex_column_expr(
                dataset, tuplify(["or", ["a", "b"]]), deepcopy(query), ParsingContext()
            )
            == "or(a, b)"
        )
        assert (
            complex_column_expr(
                dataset, tuplify(["and", ["a", "b"]]), deepcopy(query), ParsingContext()
            )
            == "and(a, b)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["or", [["or", ["a", "b"]], "c"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "or(or(a, b), c)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(["and", [["and", ["a", "b"]], "c"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "and(and(a, b), c)"
        )
        # (A OR B) AND C
        assert (
            complex_column_expr(
                dataset,
                tuplify(["and", [["or", ["a", "b"]], "c"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "and(or(a, b), c)"
        )
        # (A AND B) OR C
        assert (
            complex_column_expr(
                dataset,
                tuplify(["or", [["and", ["a", "b"]], "c"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "or(and(a, b), c)"
        )
        # A OR B OR C OR D
        assert (
            complex_column_expr(
                dataset,
                tuplify(["or", [["or", [["or", ["c", "d"]], "b"]], "a"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "or(or(or(c, d), b), a)"
        )

        assert (
            complex_column_expr(
                dataset,
                tuplify(
                    [
                        "if",
                        [
                            ["in", ["release", "tuple", ["'foo'"]]],
                            "release",
                            "'other'",
                        ],
                        "release",
                    ]
                ),
                deepcopy(query),
                ParsingContext(),
            )
            == "(if(in(release, tuple('foo')), release, 'other') AS release)"
        )
        assert (
            complex_column_expr(
                dataset,
                tuplify(
                    [
                        "if",
                        ["in", ["release", "tuple", ["'foo'"]], "release", "'other'"],
                        "release",
                    ]
                ),
                deepcopy(query),
                ParsingContext(),
            )
            == "(if(in(release, tuple('foo')), release, 'other') AS release)"
        )

        # TODO once search_message is filled in everywhere, this can be just 'message' again.
        message_expr = "(coalesce(search_message, message) AS message)"
        assert complex_column_expr(
            dataset,
            tuplify(["positionCaseInsensitive", ["message", "'lol 'single' quotes'"]]),
            deepcopy(query),
            ParsingContext(),
        ) == "positionCaseInsensitive({message_expr}, 'lol \\'single\\' quotes')".format(
            **locals()
        )

        # dangerous characters are allowed but escaped in literals and column names
        assert (
            complex_column_expr(
                dataset,
                tuplify(["safe", ["fo`o", "'ba'r'"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == r"safe(`fo\`o`, 'ba\'r')"
        )

        # Dangerous characters not allowed in functions
        with pytest.raises(AssertionError):
            assert complex_column_expr(
                dataset,
                tuplify([r"dang'erous", ["message", "`"]]),
                deepcopy(query),
                ParsingContext(),
            )

        # Or nested functions
        with pytest.raises(AssertionError):
            assert complex_column_expr(
                dataset,
                tuplify([r"safe", ["dang`erous", ["message"]]]),
                deepcopy(query),
                ParsingContext(),
            )

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_apdex_expression(self, dataset):
        body = {"aggregations": [["apdex(duration, 300)", "", "apdex_score"]]}
        parsing_context = ParsingContext()
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        exprs = [
            column_expr(dataset, col, Query(body, source), parsing_context, alias, agg)
            for (agg, col, alias) in body["aggregations"]
        ]
        assert exprs == [
            "((countIf(duration <= 300) + (countIf((duration > 300) AND (duration <= 1200)) / 2)) / count() AS apdex_score)"
        ]

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_impact_expression(self, dataset):
        body = {"aggregations": [["impact(duration, 300, user)", "", "impact_score"]]}
        parsing_context = ParsingContext()
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        exprs = [
            column_expr(dataset, col, Query(body, source), parsing_context, alias, agg)
            for (agg, col, alias) in body["aggregations"]
        ]
        assert exprs == [
            "((1 - (countIf(duration <= 300) + (countIf((duration > 300) AND (duration <= 1200)) / 2)) / count()) + ((1 - (1 / sqrt(uniq(user)))) * 3) AS impact_score)"
        ]

    @pytest.mark.parametrize("dataset", DATASETS)
    def test_failure_rate_expression(self, dataset):
        body = {"aggregations": [["failure_rate()", "", "error_percentage"]]}
        parsing_context = ParsingContext()
        source = (
            dataset.get_all_storages()[0]
            .get_schemas()
            .get_read_schema()
            .get_data_source()
        )
        exprs = [
            column_expr(dataset, col, Query(body, source), parsing_context, alias, agg)
            for (agg, col, alias) in body["aggregations"]
        ]
        assert exprs == [
            "(countIf(notIn(transaction_status, tuple(0, 1, 2))) / count() AS error_percentage)"
        ]
