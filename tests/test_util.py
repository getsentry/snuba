from datetime import date, datetime

from base import BaseTest

from snuba.util import (
    column_expr,
    complex_column_expr,
    condition_expr,
    escape_literal,
    tuplify,
)


class TestUtil(BaseTest):

    def test_column_expr(self):
        body = {
            'granularity': 86400
        }
        # Single tag expression
        assert column_expr('tags[foo]', body.copy()) ==\
            "(tags.value[indexOf(tags.key, \'foo\')] AS `tags[foo]`)"

        # Promoted tag expression / no translation
        assert column_expr('tags[server_name]', body.copy()) ==\
            "(server_name AS `tags[server_name]`)"

        # Promoted tag expression / with translation
        assert column_expr('tags[app.device]', body.copy()) ==\
            "(app_device AS `tags[app.device]`)"

        # All tag keys expression
        assert column_expr('tags_key', body.copy()) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        assert column_expr('time', body.copy()) ==\
            "(toDate(timestamp) AS time)"

        assert column_expr('col', body.copy(), aggregate='sum') ==\
            "(sum(col) AS col)"

        assert column_expr(None, body.copy(), alias='sum', aggregate='sum') ==\
            "sum"  # This should probably be an error as its an aggregate with no column

        assert column_expr('col', body.copy(), alias='summation', aggregate='sum') ==\
            "(sum(col) AS summation)"

        # Special cases where count() doesn't need a column
        assert column_expr('', body.copy(), alias='count', aggregate='count()') ==\
            "(count() AS count)"

        assert column_expr('', body.copy(), alias='aggregate', aggregate='count()') ==\
            "(count() AS aggregate)"

        # Columns that need escaping
        assert column_expr('sentry:release', body.copy()) == '`sentry:release`'

        # Columns that start with a negative sign (used in orderby to signify
        # sort order) retain the '-' sign outside the escaping backticks (if any)
        assert column_expr('-timestamp', body.copy()) == '-timestamp'
        assert column_expr('-sentry:release', body.copy()) == '-`sentry:release`'

        # A 'column' that is actually a string literal
        assert column_expr('\'hello world\'', body.copy()) == '\'hello world\''

        # Complex expressions (function calls) involving both string and column arguments
        assert column_expr(tuplify(['concat', ['a', '\':\'', 'b']]), body.copy()) == 'concat(a, \':\', b)'

        group_id_body = body.copy()
        assert column_expr('issue', group_id_body) == '(group_id AS issue)'

    def test_alias_in_alias(self):
        body = {}
        assert column_expr('tags_key', body) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        # If we want to use `tags_key` again, make sure we use the
        # already-created alias verbatim
        assert column_expr('tags_key', body) == 'tags_key'
        # If we also want to use `tags_value`, make sure that we use
        # the `all_tags` alias instead of re-expanding the tags arrayJoin
        assert column_expr('tags_value', body) == '((all_tags)[2] AS tags_value)'

    def test_escape(self):
        assert escape_literal("'") == r"'\''"
        assert escape_literal(date(2001, 1, 1)) == "toDate('2001-01-01')"
        assert escape_literal(datetime(2001, 1, 1, 1, 1, 1)) == "toDateTime('2001-01-01T01:01:01')"
        assert escape_literal([1, 'a', date(2001, 1, 1)]) ==\
            "(1, 'a', toDate('2001-01-01'))"

    def test_condition_expr(self):
        conditions = [['a', '=', 1]]
        assert condition_expr(conditions, {}) == 'a = 1'

        conditions = [[['a', '=', 1]]]
        assert condition_expr(conditions, {}) == 'a = 1'

        conditions = [['a', '=', 1], ['b', '=', 2]]
        assert condition_expr(conditions, {}) == 'a = 1 AND b = 2'

        conditions = [[['a', '=', 1], ['b', '=', 2]]]
        assert condition_expr(conditions, {}) == '(a = 1 OR b = 2)'

        conditions = [[['a', '=', 1], ['b', '=', 2]], ['c', '=', 3]]
        assert condition_expr(conditions, {}) == '(a = 1 OR b = 2) AND c = 3'

        conditions = [[['a', '=', 1], ['b', '=', 2]], [['c', '=', 3], ['d', '=', 4]]]
        assert condition_expr(conditions, {}) == '(a = 1 OR b = 2) AND (c = 3 OR d = 4)'

        # Malformed condition input
        conditions = [[['a', '=', 1], []]]
        assert condition_expr(conditions, {}) == 'a = 1'

        # Test column expansion
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        expanded = column_expr('tags[foo]', {})
        assert condition_expr(conditions, {}) == '({} = 1 OR b = 2)'.format(expanded)

        # Test using alias if column has already been expanded in SELECT clause
        reuse_body = {}
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        column_expr('tags[foo]', reuse_body)  # Expand it once so the next time is aliased
        assert condition_expr(conditions, reuse_body) == '(`tags[foo]` = 1 OR b = 2)'

        # Test special output format of LIKE
        conditions = [['primary_hash', 'LIKE', '%foo%']]
        assert condition_expr(conditions, {}) == 'primary_hash LIKE \'%foo%\''

        conditions = tuplify([[['notEmpty', ['arrayElement', ['exception_stacks.type', 1]]], '=', 1]])
        assert condition_expr(conditions, {}) == 'notEmpty(arrayElement(exception_stacks.type, 1)) = 1'

        conditions = tuplify([[['notEmpty', ['tags[sentry:user]']], '=', 1]])
        assert condition_expr(conditions, {}) == 'notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 1'

        conditions = tuplify([[['notEmpty', ['tags_key']], '=', 1]])
        assert condition_expr(conditions, {}) == 'notEmpty((((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) AS all_tags))[1] AS tags_key)) = 1'

        conditions = tuplify([
            [
                [['notEmpty', ['tags[sentry:environment]']], '=', 'dev'], [['notEmpty', ['tags[sentry:environment]']], '=', 'prod']
            ],
            [
                [['notEmpty', ['tags[sentry:user]']], '=', 'joe'], [['notEmpty', ['tags[sentry:user]']], '=', 'bob']
            ],
        ])
        assert condition_expr(conditions, {}) == \
                """(notEmpty((tags.value[indexOf(tags.key, 'sentry:environment')] AS `tags[sentry:environment]`)) = 'dev' OR notEmpty(`tags[sentry:environment]`) = 'prod') AND (notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 'joe' OR notEmpty(`tags[sentry:user]`) = 'bob')"""

    def test_duplicate_expression_alias(self):
        body = {
            'aggregations': [
                ['topK(3)', 'logger', 'dupe_alias'],
                ['uniq', 'environment', 'dupe_alias'],
            ]
        }
        # In the case where 2 different expressions are aliased
        # to the same thing, one ends up overwriting the other.
        # This may not be ideal as it may mask bugs in query conditions
        exprs = [
            column_expr(col, body, alias, agg)
            for (agg, col, alias) in body['aggregations']
        ]
        assert exprs == ['(topK(3)(logger) AS dupe_alias)', 'dupe_alias']

    def test_complex_condition_expr(self):
        body = {}

        assert complex_column_expr(tuplify(['count', []]), body.copy()) == 'count()'
        assert complex_column_expr(tuplify(['notEmpty', ['foo']]), body.copy()) == 'notEmpty(foo)'
        assert complex_column_expr(tuplify(['notEmpty', ['arrayElement', ['foo', 1]]]), body.copy()) == 'notEmpty(arrayElement(foo, 1))'
        assert complex_column_expr(tuplify(['foo', ['bar', ['qux'], 'baz']]), body.copy()) == 'foo(bar(qux), baz)'
        assert complex_column_expr(tuplify(['foo', [], 'a']), body.copy()) == '(foo() AS a)'
        assert complex_column_expr(tuplify(['foo', ['b', 'c'], 'd']), body.copy()) == '(foo(b, c) AS d)'
        assert complex_column_expr(tuplify(['foo', ['b', 'c', ['d']]]), body.copy()) == 'foo(b, c(d))'

        # we may move these to special Snuba function calls in the future
        assert complex_column_expr(tuplify(['topK', [3], ['project_id']]), body.copy()) == 'topK(3)(project_id)'
        assert complex_column_expr(tuplify(['topK', [3], ['project_id'], 'baz']), body.copy()) == '(topK(3)(project_id) AS baz)'

        assert complex_column_expr(tuplify(['emptyIfNull', ['project_id']]), body.copy()) == 'ifNull(project_id, \'\')'
        assert complex_column_expr(tuplify(['emptyIfNull', ['project_id'], 'foo']), body.copy()) == '(ifNull(project_id, \'\') AS foo)'
