from snuba.web.views import check_clickhouse


def test_something():
    assert check_clickhouse(filter_experimental=True)
