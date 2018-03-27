from datetime import date, datetime

from base import BaseTest

from snuba.util import issue_expr, column_expr, escape_literal

pad = '\x00' * 31


class TestUtil(BaseTest):

    def test_issue_expr(self):
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash') ==\
            "if(hash IN ('a%(pad)s', 'b%(pad)s'), 1, if(hash = 'c%(pad)s', 2, 0))" % {'pad': pad}
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[1]) ==\
            "if(hash IN ('a%(pad)s', 'b%(pad)s'), 1, 0)" % {'pad': pad}
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[2]) ==\
            "if(hash = 'c%(pad)s', 2, 0)" % {'pad': pad}
        assert issue_expr([(1, ['a', 'b']), (2, 'c')], col='hash', ids=[]) == 0
        assert issue_expr([], col='hash', ids=[]) == 0

    def test_column_expr(self):
        body = {
            'issues': [(1, ['a', 'b']), (2, 'c')],
        }
        assert column_expr('issue', body) ==\
            "if(primary_hash IN ('a%(pad)s', 'b%(pad)s'), 1, if(primary_hash = 'c%(pad)s', 2, 0))" % {
                'pad': pad}

        body['conditions'] = [['issue', 'IN', [1]]]
        assert column_expr('issue', body) ==\
            "if(primary_hash IN ('a%(pad)s', 'b%(pad)s'), 1, 0)" % {'pad': pad}

        body['conditions'] = [['issue', 'IN', [1]], ['issue', '=', 2]]
        assert column_expr('issue', body) ==\
            "if(primary_hash IN ('a%(pad)s', 'b%(pad)s'), 1, if(primary_hash = 'c%(pad)s', 2, 0))" % {
                'pad': pad}

        body['conditions'] = [['issue', 'IN', []]]
        assert column_expr('issue', body) == 0

    def test_escape(self):
        assert escape_literal("'") == r"'\''"
        assert escape_literal(date(2001, 1, 1)) == "toDate('2001-01-01')"
        assert escape_literal(datetime(2001, 1, 1, 1, 1, 1)) == "toDateTime('2001-01-01T01:01:01')"
        assert escape_literal([1, 'a', date(2001, 1, 1)]) ==\
            "(1, 'a', toDate('2001-01-01'))"
