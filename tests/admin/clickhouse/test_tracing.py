from snuba.admin.clickhouse.tracing import scrub_row


def test_scrub() -> None:
    assert scrub_row((1, 2, 3, "release name")) == (1, 2, 3, "<scrubbed: str>")
