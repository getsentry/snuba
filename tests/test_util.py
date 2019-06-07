from datetime import date, datetime
import pytest
import simplejson as json
import time

from base import BaseTest

from snuba.util import (
    all_referenced_columns,
    column_expr,
    complex_column_expr,
    conditions_expr,
    escape_literal,
    tuplify,
    Timer,
)


class TestUtil(BaseTest):

    def test_column_expr(self):
        body = {
            'granularity': 86400
        }
        # Single tag expression
        assert column_expr(self.dataset, 'tags[foo]', body.copy()) ==\
            "(tags.value[indexOf(tags.key, \'foo\')] AS `tags[foo]`)"

        # Promoted tag expression / no translation
        assert column_expr(self.dataset, 'tags[server_name]', body.copy()) ==\
            "(server_name AS `tags[server_name]`)"

        # Promoted tag expression / with translation
        assert column_expr(self.dataset, 'tags[app.device]', body.copy()) ==\
            "(app_device AS `tags[app.device]`)"

        # All tag keys expression
        assert column_expr(self.dataset, 'tags_key', body.copy()) == (
            '(arrayJoin(tags.key) AS tags_key)'
        )

        # If we are going to use both tags_key and tags_value, expand both
        tag_group_body = {
            'groupby': ['tags_key', 'tags_value']
        }
        assert column_expr(self.dataset, 'tags_key', tag_group_body) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        assert column_expr(self.dataset, 'time', body.copy()) ==\
            "(toDate(timestamp) AS time)"

        assert column_expr(self.dataset, 'rtime', body.copy()) ==\
            "(toDate(received) AS rtime)"

        assert column_expr(self.dataset, 'col', body.copy(), aggregate='sum') ==\
            "(sum(col) AS col)"

        assert column_expr(self.dataset, 'col', body.copy(), alias='summation', aggregate='sum') ==\
            "(sum(col) AS summation)"

        # Special cases where count() doesn't need a column
        assert column_expr(self.dataset, '', body.copy(), alias='count', aggregate='count()') ==\
            "(count() AS count)"

        assert column_expr(self.dataset, '', body.copy(), alias='aggregate', aggregate='count()') ==\
            "(count() AS aggregate)"

        # Columns that need escaping
        assert column_expr(self.dataset, 'sentry:release', body.copy()) == '`sentry:release`'

        # Columns that start with a negative sign (used in orderby to signify
        # sort order) retain the '-' sign outside the escaping backticks (if any)
        assert column_expr(self.dataset, '-timestamp', body.copy()) == '-timestamp'
        assert column_expr(self.dataset, '-sentry:release', body.copy()) == '-`sentry:release`'

        # A 'column' that is actually a string literal
        assert column_expr(self.dataset, '\'hello world\'', body.copy()) == '\'hello world\''

        # Complex expressions (function calls) involving both string and column arguments
        assert column_expr(self.dataset, tuplify(['concat', ['a', '\':\'', 'b']]), body.copy()) == 'concat(a, \':\', b)'

        group_id_body = body.copy()
        assert column_expr(self.dataset, 'issue', group_id_body) == '(group_id AS issue)'

        # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where a number was expected.
        assert column_expr(self.dataset, 'tags[environment]', body.copy(), alias='unique_envs', aggregate='uniq') == "(ifNull(uniq(environment), 0) AS unique_envs)"

    def test_alias_in_alias(self):
        body = {
            'groupby': ['tags_key', 'tags_value']
        }
        assert column_expr(self.dataset, 'tags_key', body) == (
            '(((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) '
            'AS all_tags))[1] AS tags_key)'
        )

        # If we want to use `tags_key` again, make sure we use the
        # already-created alias verbatim
        assert column_expr(self.dataset, 'tags_key', body) == 'tags_key'
        # If we also want to use `tags_value`, make sure that we use
        # the `all_tags` alias instead of re-expanding the tags arrayJoin
        assert column_expr(self.dataset, 'tags_value', body) == '((all_tags)[2] AS tags_value)'

    def test_escape(self):
        assert escape_literal(r"'") == r"'\''"
        assert escape_literal(r"\'") == r"'\\\''"
        assert escape_literal(date(2001, 1, 1)) == "toDate('2001-01-01')"
        assert escape_literal(datetime(2001, 1, 1, 1, 1, 1)) == "toDateTime('2001-01-01T01:01:01')"
        assert escape_literal([1, 'a', date(2001, 1, 1)]) ==\
            "(1, 'a', toDate('2001-01-01'))"

    def test_conditions_expr(self):
        conditions = [['a', '=', 1]]
        assert conditions_expr(self.dataset, conditions, {}) == 'a = 1'

        conditions = [[['a', '=', 1]]]
        assert conditions_expr(self.dataset, conditions, {}) == 'a = 1'

        conditions = [['a', '=', 1], ['b', '=', 2]]
        assert conditions_expr(self.dataset, conditions, {}) == 'a = 1 AND b = 2'

        conditions = [[['a', '=', 1], ['b', '=', 2]]]
        assert conditions_expr(self.dataset, conditions, {}) == '(a = 1 OR b = 2)'

        conditions = [[['a', '=', 1], ['b', '=', 2]], ['c', '=', 3]]
        assert conditions_expr(self.dataset, conditions, {}) == '(a = 1 OR b = 2) AND c = 3'

        conditions = [[['a', '=', 1], ['b', '=', 2]], [['c', '=', 3], ['d', '=', 4]]]
        assert conditions_expr(self.dataset, conditions, {}) == '(a = 1 OR b = 2) AND (c = 3 OR d = 4)'

        # Malformed condition input
        conditions = [[['a', '=', 1], []]]
        assert conditions_expr(self.dataset, conditions, {}) == 'a = 1'

        # Test column expansion
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        expanded = column_expr(self.dataset, 'tags[foo]', {})
        assert conditions_expr(self.dataset, conditions, {}) == '({} = 1 OR b = 2)'.format(expanded)

        # Test using alias if column has already been expanded in SELECT clause
        reuse_body = {}
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        column_expr(self.dataset, 'tags[foo]', reuse_body)  # Expand it once so the next time is aliased
        assert conditions_expr(self.dataset, conditions, reuse_body) == '(`tags[foo]` = 1 OR b = 2)'

        # Test special output format of LIKE
        conditions = [['primary_hash', 'LIKE', '%foo%']]
        assert conditions_expr(self.dataset, conditions, {}) == 'primary_hash LIKE \'%foo%\''

        conditions = tuplify([[['notEmpty', ['arrayElement', ['exception_stacks.type', 1]]], '=', 1]])
        assert conditions_expr(self.dataset, conditions, {}) == 'notEmpty(arrayElement(exception_stacks.type, 1)) = 1'

        conditions = tuplify([[['notEmpty', ['tags[sentry:user]']], '=', 1]])
        assert conditions_expr(self.dataset, conditions, {}) == 'notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 1'

        conditions = tuplify([[['notEmpty', ['tags_key']], '=', 1]])
        assert conditions_expr(self.dataset, conditions, {}) == 'notEmpty((arrayJoin(tags.key) AS tags_key)) = 1'

        conditions = tuplify([
            [
                [['notEmpty', ['tags[sentry:environment]']], '=', 'dev'], [['notEmpty', ['tags[sentry:environment]']], '=', 'prod']
            ],
            [
                [['notEmpty', ['tags[sentry:user]']], '=', 'joe'], [['notEmpty', ['tags[sentry:user]']], '=', 'bob']
            ],
        ])
        assert conditions_expr(self.dataset, conditions, {}) == \
            """(notEmpty((tags.value[indexOf(tags.key, 'sentry:environment')] AS `tags[sentry:environment]`)) = 'dev' OR notEmpty(`tags[sentry:environment]`) = 'prod') AND (notEmpty((`sentry:user` AS `tags[sentry:user]`)) = 'joe' OR notEmpty(`tags[sentry:user]`) = 'bob')"""

        # Test scalar condition on array column is expanded as an iterator.
        conditions = [['exception_frames.filename', 'LIKE', '%foo%']]
        assert conditions_expr(self.dataset, conditions, {}) == 'arrayExists(x -> assumeNotNull(x LIKE \'%foo%\'), exception_frames.filename)'

        # Test negative scalar condition on array column is expanded as an all() type iterator.
        conditions = [['exception_frames.filename', 'NOT LIKE', '%foo%']]
        assert conditions_expr(self.dataset, conditions, {}) == 'arrayAll(x -> assumeNotNull(x NOT LIKE \'%foo%\'), exception_frames.filename)'

        # Test that a duplicate IN condition is deduplicated even if
        # the lists are in different orders.[
        conditions = tuplify([
            ['platform', 'IN', ['a', 'b', 'c']],
            ['platform', 'IN', ['c', 'b', 'a']]
        ])
        assert conditions_expr(self.dataset, conditions, {}) == "platform IN ('a', 'b', 'c')"

    def test_duplicate_expression_alias(self):
        body = {
            'aggregations': [
                ['top3', 'logger', 'dupe_alias'],
                ['uniq', 'environment', 'dupe_alias'],
            ]
        }
        # In the case where 2 different expressions are aliased
        # to the same thing, one ends up overwriting the other.
        # This may not be ideal as it may mask bugs in query conditions
        exprs = [
            column_expr(self.dataset, col, body, alias, agg)
            for (agg, col, alias) in body['aggregations']
        ]
        assert exprs == ['(topK(3)(logger) AS dupe_alias)', 'dupe_alias']

    def test_nested_aggregate_legacy_format(self):
        priority = ['toUInt64(plus(multiply(log(times_seen), 600), last_seen))', '', 'priority']
        assert column_expr(self.dataset, '', {'aggregations': [priority]}, priority[2], priority[0]) == '(toUInt64(plus(multiply(log(times_seen), 600), last_seen)) AS priority)'

        top_k = ['topK(3)', 'logger', 'top_3']
        assert column_expr(self.dataset, top_k[1], {'aggregations': [top_k]}, top_k[2], top_k[0]) == '(topK(3)(logger) AS top_3)'

    def test_complex_conditions_expr(self):
        body = {}

        assert complex_column_expr(self.dataset, tuplify(['count', []]), body.copy()) == 'count()'
        assert complex_column_expr(self.dataset, tuplify(['notEmpty', ['foo']]), body.copy()) == 'notEmpty(foo)'
        assert complex_column_expr(self.dataset, tuplify(['notEmpty', ['arrayElement', ['foo', 1]]]), body.copy()) == 'notEmpty(arrayElement(foo, 1))'
        assert complex_column_expr(self.dataset, tuplify(['foo', ['bar', ['qux'], 'baz']]), body.copy()) == 'foo(bar(qux), baz)'
        assert complex_column_expr(self.dataset, tuplify(['foo', [], 'a']), body.copy()) == '(foo() AS a)'
        assert complex_column_expr(self.dataset, tuplify(['foo', ['b', 'c'], 'd']), body.copy()) == '(foo(b, c) AS d)'
        assert complex_column_expr(self.dataset, tuplify(['foo', ['b', 'c', ['d']]]), body.copy()) == 'foo(b, c(d))'

        assert complex_column_expr(self.dataset, tuplify(['top3', ['project_id']]), body.copy()) == 'topK(3)(project_id)'
        assert complex_column_expr(self.dataset, tuplify(['top10', ['project_id'], 'baz']), body.copy()) == '(topK(10)(project_id) AS baz)'

        assert complex_column_expr(self.dataset, tuplify(['emptyIfNull', ['project_id']]), body.copy()) == 'ifNull(project_id, \'\')'
        assert complex_column_expr(self.dataset, tuplify(['emptyIfNull', ['project_id'], 'foo']), body.copy()) == '(ifNull(project_id, \'\') AS foo)'

        assert complex_column_expr(self.dataset, tuplify(['or', ['a', 'b']]), body.copy()) == 'or(a, b)'
        assert complex_column_expr(self.dataset, tuplify(['and', ['a', 'b']]), body.copy()) == 'and(a, b)'
        assert complex_column_expr(self.dataset, tuplify(['or', [['or', ['a', 'b']], 'c']]), body.copy()) == 'or(or(a, b), c)'
        assert complex_column_expr(self.dataset, tuplify(['and', [['and', ['a', 'b']], 'c']]), body.copy()) == 'and(and(a, b), c)'
        # (A OR B) AND C
        assert complex_column_expr(self.dataset, tuplify(['and', [['or', ['a', 'b']], 'c']]), body.copy()) == 'and(or(a, b), c)'
        # (A AND B) OR C
        assert complex_column_expr(self.dataset, tuplify(['or', [['and', ['a', 'b']], 'c']]), body.copy()) == 'or(and(a, b), c)'
        # A OR B OR C OR D
        assert complex_column_expr(self.dataset, tuplify(['or', [['or', [['or', ['c', 'd']], 'b']], 'a']]), body.copy()) == 'or(or(or(c, d), b), a)'

        assert complex_column_expr(self.dataset, tuplify(['if', [['in', ['release', 'tuple', ["'foo'"], ], ], 'release', "'other'"], 'release', ]), body.copy()) == "(if(in(release, tuple('foo')), release, 'other') AS release)"
        assert complex_column_expr(self.dataset, tuplify(['if', ['in', ['release', 'tuple', ["'foo'"]], 'release', "'other'", ], 'release']), body.copy()) == "(if(in(release, tuple('foo')), release, 'other') AS release)"

        # TODO once search_message is filled in everywhere, this can be just 'message' again.
        message_expr = '(coalesce(search_message, message) AS message)'
        assert complex_column_expr(self.dataset, tuplify(['positionCaseInsensitive', ['message', "'lol 'single' quotes'"]]), body.copy())\
            == "positionCaseInsensitive({message_expr}, 'lol \\'single\\' quotes')".format(**locals())

        # dangerous characters are allowed but escaped in literals and column names
        assert complex_column_expr(self.dataset, tuplify(['safe', ['fo`o', "'ba'r'"]]), body.copy()) == r"safe(`fo\`o`, 'ba\'r')"

        # Dangerous characters not allowed in functions
        with pytest.raises(AssertionError):
            assert complex_column_expr(self.dataset, tuplify([r"dang'erous", ['message', '`']]), body.copy())

        # Or nested functions
        with pytest.raises(AssertionError):
            assert complex_column_expr(self.dataset, tuplify([r"safe", ['dang`erous', ['message']]]), body.copy())

    def test_referenced_columns(self):
        # a = 1 AND b = 1
        body = {
            'conditions': [
                ['a', '=', '1'],
                ['b', '=', '1'],
            ]
        }
        assert all_referenced_columns(body) == set(['a', 'b'])

        # a = 1 AND (b = 1 OR c = 1)
        body = {
            'conditions': [
                ['a', '=', '1'],
                [
                    ['b', '=', '1'],
                    ['c', '=', '1'],
                ],
            ]
        }
        assert all_referenced_columns(body) == set(['a', 'b', 'c'])

        # a = 1 AND (b = 1 OR foo(c) = 1)
        body = {
            'conditions': [
                ['a', '=', '1'],
                [
                    ['b', '=', '1'],
                    [['foo', ['c']], '=', '1'],
                ],
            ]
        }
        assert all_referenced_columns(body) == set(['a', 'b', 'c'])

        # a = 1 AND (b = 1 OR foo(c, bar(d)) = 1)
        body = {
            'conditions': [
                ['a', '=', '1'],
                [
                    ['b', '=', '1'],
                    [['foo', ['c', ['bar', ['d']]]], '=', '1'],
                ],
            ]
        }
        assert all_referenced_columns(body) == set(['a', 'b', 'c', 'd'])

        # Other fields, including expressions in selected columns
        body = {
            'arrayjoin': 'tags_key',
            'groupby': ['time', 'issue'],
            'orderby': '-time',
            'selected_columns': [
                'issue',
                'time',
                ['foo', ['c', ['bar', ['d']]]]  # foo(c, bar(d))
            ],
            'aggregations': [
                ['uniq', 'tags_value', 'values_seen']
            ]
        }
        assert all_referenced_columns(body) == set(['tags_key', 'tags_value', 'time', 'issue', 'c', 'd'])

    def test_timer(self):
        t = Timer()
        time.sleep(0.001)
        t.mark('thing1')
        time.sleep(0.001)
        t.mark('thing2')
        snapshot = t.finish()

        # Test that we can add more time under the same marks and the time will
        # be cumulatively added under those keys.
        time.sleep(0.001)
        t.mark('thing1')
        time.sleep(0.001)
        t.mark('thing2')
        snapshot_2 = t.finish()

        assert snapshot['marks_ms'].keys() == snapshot_2['marks_ms'].keys()
        assert snapshot['marks_ms']['thing1'] < snapshot_2['marks_ms']['thing1']
        assert snapshot['marks_ms']['thing2'] < snapshot_2['marks_ms']['thing2']
