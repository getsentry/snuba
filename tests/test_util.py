from snuba.clickhouse.escaping import escape_alias, escape_identifier


def test_escape_identifier() -> None:
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


def test_escape_alias() -> None:
    assert escape_alias(None) is None
    assert escape_alias("") == ""
    assert escape_alias("foo") == "foo"
    assert escape_alias("foo.bar") == "`foo.bar`"
    assert escape_alias("foo:bar") == "`foo:bar`"
