import pytest

from snuba.utils.kafka.configuration import get_bool_configuration_value


def test_get_bool_configuration_value():
    assert get_bool_configuration_value({"key": "t"}, "key", False) == True
    assert get_bool_configuration_value({"key": "true"}, "key", False) == True
    assert get_bool_configuration_value({"key": "1"}, "key", False) == True
    assert get_bool_configuration_value({"key": True}, "key", False) == True

    assert get_bool_configuration_value({"key": "f"}, "key", True) == False
    assert get_bool_configuration_value({"key": "false"}, "key", True) == False
    assert get_bool_configuration_value({"key": "0"}, "key", True) == False
    assert get_bool_configuration_value({"key": False}, "key", True) == False

    assert get_bool_configuration_value({"key": "TRUE"}, "key", False) == True

    with pytest.raises(ValueError):
        assert get_bool_configuration_value({"key": "ok"}, "key", True)

    with pytest.raises(TypeError):
        assert get_bool_configuration_value({"key": None}, "key", True)
