from base import BaseTest

from snuba.util import *


class TestUtil(BaseTest):

    def test_issue_expr(self):
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash') ==\
            "[1,1,2][indexOf([toFixedString('a',32),toFixedString('b',32),toFixedString('c',32)], hash)]"
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[1]) ==\
            "[1,1][indexOf([toFixedString('a',32),toFixedString('b',32)], hash)]"
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[2]) ==\
            "[2][indexOf([toFixedString('c',32)], hash)]"
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[]) == 0
        assert issue_expr([], col='hash', ids=[]) == 0

    def test_column_expr(self):
        body = {
            'issues': [(1, ['a', 'b']), (2, 'c')],
        }
        assert column_expr('issue', body.copy()) ==\
            "([1,1,2][indexOf([toFixedString('a',32),toFixedString('b',32),toFixedString('c',32)], primary_hash)] AS `issue`)"

        body['conditions'] = [['issue', 'IN', [1]]]
        assert column_expr('issue', body.copy()) ==\
            "([1,1][indexOf([toFixedString('a',32),toFixedString('b',32)], primary_hash)] AS `issue`)"

        body['conditions'] = [['issue', 'IN', []]]
        assert column_expr('issue', body.copy()) == "(0 AS `issue`)"

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
