from base import BaseTest

from snuba.util import *


class TestUtil(BaseTest):

    def test_column_expr(self):
        body = {
            'granularity': 86400
        }
        assert column_expr('tags[foo]', body.copy()) ==\
            "(tags.value[indexOf(tags.key, \'foo\')] AS `tags[foo]`)"

        assert column_expr('time', body.copy()) ==\
            "(toDate(timestamp) AS `time`)"

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
