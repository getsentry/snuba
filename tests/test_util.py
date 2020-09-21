from datetime import date, datetime

from snuba.clickhouse.escaping import escape_alias, escape_identifier
from snuba.util import escape_literal
from tests.base import BaseTest


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
