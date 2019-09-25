import pytest

from snuba.utils.kafka.configuration import (
    get_bool_configuration_value,
    get_enum_configuration_value,
)


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


def test_get_enum_configuration_value():
    assert (
        get_enum_configuration_value(
            {}, "auto.offset.reset", {"earliest": set(["beginning"])}, "earliest"
        )
        == "earliest"
    )
    assert (
        get_enum_configuration_value(
            {}, "auto.offset.reset", {"earliest": set(["beginning"])}, "beginning"
        )
        == "earliest"
    )

    assert (
        get_enum_configuration_value(
            {"auto.offset.reset": "earliest"},
            "auto.offset.reset",
            {"earliest": set([])},
            "",
        )
        == "earliest"
    )
    assert (
        get_enum_configuration_value(
            {"auto.offset.reset": "beginning"},
            "auto.offset.reset",
            {"earliest": set(["beginning"])},
            "",
        )
        == "earliest"
    )

    with pytest.raises(ValueError):
        assert get_enum_configuration_value(
            {"auto.offset.reset": "invalid"},
            "auto.offset.reset",
            {"earliest": set(["beginning"])},
            "",
        )

    with pytest.raises(TypeError):
        assert get_enum_configuration_value(
            {"auto.offset.reset": None},
            "auto.offset.reset",
            {"earliest": set(["beginning"])},
            "",
        )
