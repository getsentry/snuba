def test_auto_import() -> None:
    from snuba.clickhouse.translators.snuba.allowed import FunctionCallMapper

    assert FunctionCallMapper.get_from_name("AggregateFunctionMapper") is not None
