from base import BaseEventsTest

from snuba.query.parsing import ParsingContext
from snuba.util import (
    column_expr,
    tuplify,
)


class TestEventsDataset(BaseEventsTest):

    def test_column_expr(self):
        body = {
            'granularity': 86400
        }

        # Single tag expression
        assert column_expr(self.dataset, 'tags[foo]', body.copy(), ParsingContext()) ==\
            "(tags.value[indexOf(tags.key, \'foo\')] AS `tags[foo]`)"

        # Promoted tag expression / no translation
        assert column_expr(self.dataset, 'tags[server_name]', body.copy(), ParsingContext()) ==\
            "(server_name AS `tags[server_name]`)"

        # Promoted tag expression / with translation
        assert column_expr(self.dataset, 'tags[app.device]', body.copy(), ParsingContext()) ==\
            "(app_device AS `tags[app.device]`)"

        # All tag keys expression
        assert column_expr(self.dataset, 'tags_key', body.copy(), ParsingContext()) == (
            '(arrayJoin(tags.key) AS tags_key)'
        )

        # If we are going to use both tags_key and tags_value, expand both
        tag_group_body = {
            'groupby': ['tags_key', 'tags_value']
        }
        assert column_expr(self.dataset, 'tags_key', tag_group_body, ParsingContext()) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        assert column_expr(self.dataset, 'time', body.copy(), ParsingContext()) ==\
            "(toDate(timestamp) AS time)"

        assert column_expr(self.dataset, 'rtime', body.copy(), ParsingContext()) ==\
            "(toDate(received) AS rtime)"

        assert column_expr(self.dataset, 'col', body.copy(), ParsingContext(), aggregate='sum') ==\
            "(sum(col) AS col)"

        assert column_expr(self.dataset, 'col', body.copy(), ParsingContext(), alias='summation', aggregate='sum') ==\
            "(sum(col) AS summation)"

        # Special cases where count() doesn't need a column
        assert column_expr(self.dataset, '', body.copy(), ParsingContext(), alias='count', aggregate='count()') ==\
            "(count() AS count)"

        assert column_expr(self.dataset, '', body.copy(), ParsingContext(), alias='aggregate', aggregate='count()') ==\
            "(count() AS aggregate)"

        # Columns that need escaping
        assert column_expr(self.dataset, 'sentry:release', body.copy(), ParsingContext()) == '`sentry:release`'

        # Columns that start with a negative sign (used in orderby to signify
        # sort order) retain the '-' sign outside the escaping backticks (if any)
        assert column_expr(self.dataset, '-timestamp', body.copy(), ParsingContext()) == '-timestamp'
        assert column_expr(self.dataset, '-sentry:release', body.copy(), ParsingContext()) == '-`sentry:release`'

        # A 'column' that is actually a string literal
        assert column_expr(self.dataset, '\'hello world\'', body.copy(), ParsingContext()) == '\'hello world\''

        # Complex expressions (function calls) involving both string and column arguments
        assert column_expr(self.dataset, tuplify(['concat', ['a', '\':\'', 'b']]), body.copy(), ParsingContext()) == 'concat(a, \':\', b)'

        group_id_body = body.copy()
        assert column_expr(self.dataset, 'issue', group_id_body, ParsingContext()) == '(nullIf(group_id, 0) AS issue)'
        assert column_expr(self.dataset, 'group_id', group_id_body, ParsingContext()) == '(nullIf(group_id, 0) AS group_id)'

        # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where a number was expected.
        assert column_expr(self.dataset, 'tags[environment]', body.copy(), ParsingContext(), alias='unique_envs', aggregate='uniq') == "(ifNull(uniq(environment), 0) AS unique_envs)"

    def test_alias_in_alias(self):
        body = {
            'groupby': ['tags_key', 'tags_value']
        }
        context = ParsingContext()
        assert column_expr(self.dataset, 'tags_key', body, context) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        # If we want to use `tags_key` again, make sure we use the
        # already-created alias verbatim
        assert column_expr(self.dataset, 'tags_key', body, context) == 'tags_key'
        # If we also want to use `tags_value`, make sure that we use
        # the `all_tags` alias instead of re-expanding the tags arrayJoin
        assert column_expr(self.dataset, 'tags_value', body, context) == '((all_tags)[2] AS tags_value)'
