import pytest
from scripts import copy_tables


@pytest.mark.parametrize(
    "table_statement, expected_match",
    [
        (
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/transactions-{shard}/transactions_local', '{replica}', deleted)",
            "/clickhouse/tables/transactions-{shard}/transactions_local",
        ),
        (
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/transactions-{shard}/transactions_local', '{replica}')",
            "/clickhouse/tables/transactions-{shard}/transactions_local",
        ),
        (
            "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/transactions-{shard}/transactions_2_CAPITAL_local', '{replica}')",
            "/clickhouse/tables/transactions-{shard}/transactions_2_CAPITAL_local",
        ),
    ],
)
def test_copy_tables_regex(table_statement: str, expected_match: str) -> None:
    """
    Verify that the regex for extracting the zk path works as expected
    """
    assert copy_tables.get_regex_match(table_statement) == expected_match
