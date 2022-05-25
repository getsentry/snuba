from snuba.web.views import check_clickhouse


def test_check_clickhouse() -> None:
    assert check_clickhouse(filter_experimental=True)
    assert not check_clickhouse(filter_experimental=False)
