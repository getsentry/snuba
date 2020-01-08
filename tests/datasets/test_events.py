from copy import deepcopy

from tests.base import BaseEventsTest

from snuba.query.columns import column_expr
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.util import tuplify


class TestEventsDataset(BaseEventsTest):
    def test_column_expr(self):
        source = self.dataset.get_dataset_schemas().get_read_schema().get_data_source()
        query = Query({"granularity": 86400}, source,)
        # Single tag expression
        assert (
            column_expr(self.dataset, "tags[foo]", deepcopy(query), ParsingContext())
            == "(tags.value[indexOf(tags.key, 'foo')] AS `tags[foo]`)"
        )

        # Promoted tag expression / no translation
        assert (
            column_expr(
                self.dataset, "tags[server_name]", deepcopy(query), ParsingContext()
            )
            == "(server_name AS `tags[server_name]`)"
        )

        # Promoted tag expression / with translation
        assert (
            column_expr(
                self.dataset, "tags[app.device]", deepcopy(query), ParsingContext()
            )
            == "(app_device AS `tags[app.device]`)"
        )

        # All tag keys expression
        assert column_expr(
            self.dataset, "tags_key", deepcopy(query), ParsingContext()
        ) == ("(arrayJoin(tags.key) AS tags_key)")

        # If we are going to use both tags_key and tags_value, expand both
        tag_group_body = {"groupby": ["tags_key", "tags_value"]}
        assert column_expr(
            self.dataset, "tags_key", Query(tag_group_body, source), ParsingContext()
        ) == (
            "(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) "
            "AS all_tags))[1] AS tags_key)"
        )

        assert (
            column_expr(self.dataset, "time", deepcopy(query), ParsingContext())
            == "(toDate(timestamp) AS time)"
        )

        assert (
            column_expr(self.dataset, "rtime", deepcopy(query), ParsingContext())
            == "(toDate(received) AS rtime)"
        )

        assert (
            column_expr(
                self.dataset, "col", deepcopy(query), ParsingContext(), aggregate="sum"
            )
            == "(sum(col) AS col)"
        )

        assert (
            column_expr(
                self.dataset,
                "col",
                deepcopy(query),
                ParsingContext(),
                alias="summation",
                aggregate="sum",
            )
            == "(sum(col) AS summation)"
        )

        # Special cases where count() doesn't need a column
        assert (
            column_expr(
                self.dataset,
                "",
                deepcopy(query),
                ParsingContext(),
                alias="count",
                aggregate="count()",
            )
            == "(count() AS count)"
        )

        assert (
            column_expr(
                self.dataset,
                "",
                deepcopy(query),
                ParsingContext(),
                alias="aggregate",
                aggregate="count()",
            )
            == "(count() AS aggregate)"
        )

        # Columns that need escaping
        assert (
            column_expr(
                self.dataset, "sentry:release", deepcopy(query), ParsingContext()
            )
            == "`sentry:release`"
        )

        # A 'column' that is actually a string literal
        assert (
            column_expr(
                self.dataset, "'hello world'", deepcopy(query), ParsingContext()
            )
            == "'hello world'"
        )

        # Complex expressions (function calls) involving both string and column arguments
        assert (
            column_expr(
                self.dataset,
                tuplify(["concat", ["a", "':'", "b"]]),
                deepcopy(query),
                ParsingContext(),
            )
            == "concat(a, ':', b)"
        )

        group_id_query = deepcopy(query)
        assert (
            column_expr(self.dataset, "group_id", group_id_query, ParsingContext())
            == "(nullIf(group_id, 0) AS group_id)"
        )

        # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where a number was expected.
        assert (
            column_expr(
                self.dataset,
                "tags[environment]",
                deepcopy(query),
                ParsingContext(),
                alias="unique_envs",
                aggregate="uniq",
            )
            == "(ifNull(uniq(environment), 0) AS unique_envs)"
        )

    def test_alias_in_alias(self):
        source = self.dataset.get_dataset_schemas().get_read_schema().get_data_source()
        query = Query({"groupby": ["tags_key", "tags_value"]}, source,)
        context = ParsingContext()
        assert column_expr(self.dataset, "tags_key", query, context) == (
            "(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) "
            "AS all_tags))[1] AS tags_key)"
        )

        # If we want to use `tags_key` again, make sure we use the
        # already-created alias verbatim
        assert column_expr(self.dataset, "tags_key", query, context) == "tags_key"
        # If we also want to use `tags_value`, make sure that we use
        # the `all_tags` alias instead of re-expanding the tags arrayJoin
        assert (
            column_expr(self.dataset, "tags_value", query, context)
            == "((all_tags)[2] AS tags_value)"
        )

    def test_order_by(self):
        """
        Order by in Snuba are represented as -COL_NAME when ordering DESC.
        since the column is provided with the `-` character in front when reaching
        the column_expr call, this can introduce a ton of corner cases depending
        whether the column is aliased, whether it gets processed into something
        else or whether it is escaped.

        This test is supposed to cover those cases.
        """
        source = self.dataset.get_dataset_schemas().get_read_schema().get_data_source()
        query = Query({}, source)
        # Columns that start with a negative sign (used in orderby to signify
        # sort order) retain the '-' sign outside the escaping backticks (if any)
        assert (
            column_expr(self.dataset, "-timestamp", deepcopy(query), ParsingContext())
            == "-timestamp"
        )
        assert (
            column_expr(
                self.dataset, "-sentry:release", deepcopy(query), ParsingContext()
            )
            == "-`sentry:release`"
        )

        context = ParsingContext()
        context.add_alias("al1")
        assert (
            column_expr(self.dataset, "-timestamp", deepcopy(query), context, "al1")
            == "-al1"
        )

        assert (
            column_expr(
                self.dataset, "-timestamp", deepcopy(query), ParsingContext(), "al1"
            )
            == "-(timestamp AS al1)"
        )

        assert (
            column_expr(
                self.dataset,
                "-exception_stacks.type",
                deepcopy(query),
                ParsingContext(),
            )
            == "-(exception_stacks.type AS `exception_stacks.type`)"
        )

        context = ParsingContext()
        context.add_alias("`exception_stacks.type`")
        assert (
            column_expr(
                self.dataset, "-exception_stacks.type", deepcopy(query), context,
            )
            == "-`exception_stacks.type`"
        )
