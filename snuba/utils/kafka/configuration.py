from typing import Any, Mapping


def get_bool_configuration_value(
    configuration: Mapping[str, Any], key: str, default: bool
) -> bool:
    """
    Return the value of a boolean in a librdkafka configuration dictionary as
    a Python bool.
    """
    # https://github.com/edenhill/librdkafka/blob/c8293af/src/rdkafka_conf.c#L1633-L1660
    value = configuration.get(key, default)
    if isinstance(value, str):
        value = value.lower().strip()
        if value in {"true", "t", "1"}:
            return True
        elif value in {"false", "f", "0"}:
            return False
    elif isinstance(value, bool):
        return value

    raise TypeError(f"unexpected value for {key!r}")
