from base import BaseTest

from snuba import util
from snuba.util import *
from mock import patch


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
        body = {
            'issues': [(1, ['a', 'b']), (2, 'c')],
        }

        conditions = [['a', '=', 1]]
        assert condition_expr(conditions, body.copy()) == 'a = 1'

        conditions = [[['a', '=', 1]]]
        assert condition_expr(conditions, body.copy()) == 'a = 1'

        conditions = [['a', '=', 1], ['b', '=', 2]]
        assert condition_expr(conditions, body.copy()) == 'a = 1 AND b = 2'

        conditions = [[['a', '=', 1], ['b', '=', 2]]]
        assert condition_expr(conditions, body.copy()) == '(a = 1 OR b = 2)'

        conditions = [[['a', '=', 1], ['b', '=', 2]], ['c', '=', 3]]
        assert condition_expr(conditions, body.copy()) == '(a = 1 OR b = 2) AND c = 3'

        conditions = [[['a', '=', 1], ['b', '=', 2]], [['c', '=', 3], ['d', '=', 4]]]
        assert condition_expr(conditions, body.copy()) == '(a = 1 OR b = 2) AND (c = 3 OR d = 4)'

        # Malformed condition input
        conditions = [[['a', '=', 1], []]]
        assert condition_expr(conditions, body.copy()) == 'a = 1'

        # Test column expansion
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        expanded = column_expr('tags[foo]', body.copy())
        assert condition_expr(conditions, body.copy()) == '({} = 1 OR b = 2)'.format(expanded)

        # Test using alias if column has already been expanded in SELECT clause
        conditions = [[['tags[foo]', '=', 1], ['b', '=', 2]]]
        column_expr('tags[foo]', body)  # Expand it once so the next time is aliased
        assert condition_expr(conditions, body) == '(`tags[foo]` = 1 OR b = 2)'

        # Test special output format of LIKE
        conditions = [['primary_hash', 'LIKE', '%foo%']]
        assert condition_expr(conditions, body.copy()) == 'like(toString(primary_hash), \'%foo%\')'

    def test_duplicate_expression_alias(self):
        body = {
            'issues': [(1, ['a', 'b']), (2, 'c')],
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

    def test_issue_expr(self):
        # Provides list of issues but doesn't use them
        body = {
            'issues': [(1, 2, ['a', 'b']), (2, 3, 'c')],
        }
        assert issue_expr(body) == ''

        # Uses issue in groupby, expands all issues
        body = {
            'issues': [(1, 2, ['a', 'b']), (2, 3, 'c')],
            'groupby': ['timestamp', 'issue']
        }
        assert '[1,1,2]' in issue_expr(body)

        # Issue in condition, expands only that issue
        body = {
            'issues': [(1, 2, ['a', 'b']), (2, 3, 'c')],
            'conditions': [['issue', '=', 1]]
        }
        assert '[1,1]' in issue_expr(body)

        # Issue in aggregation, expands all.
        body = {
            'issues': [(1, 2, ['a', 'b']), (2, 3, 'c')],
            'aggregations': [['topK(3)', 'issue', 'top_issues']]
        }
        assert '[1,1,2]' in issue_expr(body)

        # No issues to expand, and no reference to any specific issue, but
        # still need `issue` defined for groupby so we expand the issue
        # expression with empty lists
        body = {
            'issues': [],
            'groupby': ['issue']
        }
        assert 'ANY INNER JOIN' in issue_expr(body)
        assert '[]' in issue_expr(body)

        # No issues to expand, but a condition on a specific issue. Creates
        # issue join expresssion with single Null value to avoid comparison bug.
        body = {
            'issues': [],
            'conditions': [['issue', '=', 4]]
        }
        assert 'ANY INNER JOIN' in issue_expr(body)
        assert '[Null]' in issue_expr(body)

        # Ensure project_id expression is in the INNER JOIN
        body = {
            'issues': [(1, 99, ['a', 'b']), (2, 100, 'c')],
            'conditions': [['issue', 'IN', [1, 2]]]
        }
        assert '[1,1,2]' in issue_expr(body)
        assert '[99,99,100]' in issue_expr(body)
