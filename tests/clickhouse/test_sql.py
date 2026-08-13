from snuba.clickhouse.sql import identifier, on_cluster_clause


def test_on_cluster_clause() -> None:
    assert on_cluster_clause("test_cluster") == " ON CLUSTER 'test_cluster'"
    # Empty and None both mean "no cluster", so callers can concatenate blindly.
    assert on_cluster_clause(None) == ""
    assert on_cluster_clause("") == ""


def test_on_cluster_clause_escapes() -> None:
    assert on_cluster_clause("a'b") == " ON CLUSTER 'a\\'b'"
    assert on_cluster_clause("a\\b") == " ON CLUSTER 'a\\\\b'"


def test_identifier() -> None:
    assert identifier("my_table") == "my_table"
    # Anything outside the safe shape gets backticked.
    assert identifier("weird table") == "`weird table`"
    assert identifier("a`b") == "`a\\`b`"


def test_identifier_passes_empty_through() -> None:
    # escape_identifier returns falsy input unchanged rather than None. Kept as
    # is so this helper only removes the None from the signature.
    assert identifier("") == ""
