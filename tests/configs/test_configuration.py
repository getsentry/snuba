import pytest

from snuba.configs.configuration import Configuration


def test_float_config_accepts_int_default() -> None:
    config = Configuration("divider", "", float, 2)
    assert config.default == 2.0 and type(config.default) is float


def test_other_type_mismatch_still_raises() -> None:
    with pytest.raises(ValueError):
        Configuration("limit", "", int, 2.5)
