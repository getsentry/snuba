def test_auto_import():
    from snuba.clickhouse.translators.snuba.function_call_mappers import (
        FunctionCallMapper,
    )

    assert FunctionCallMapper.get_from_name("aggregate_function") is not None
