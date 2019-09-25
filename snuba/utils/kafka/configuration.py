from typing import Any, Mapping, Set


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
        value = value.lower()
        if value in {"true", "t", "1"}:
            return True
        elif value in {"false", "f", "0"}:
            return False
        else:
            raise ValueError(f"unexpected value for {key!r}")
    elif isinstance(value, bool):
        return value

    raise TypeError(f"unexpected value for {key!r}")


def get_enum_configuration_value(
    configuration: Mapping[str, Any],
    key: str,
    values: Mapping[str, Set[str]],
    default: str,
) -> str:
    """
    Return the value of a value in a librdkafka configuration dictionary as
    the canonical name from a mapping of canonical names to a set of aliases.
    """
    value = configuration.get(key, default)
    if isinstance(value, str):
        value = value.lower()

        if value in values:
            return value

        for canonical, aliases in values.items():
            if value in aliases:
                return canonical

        raise ValueError(f"unexpected value for {key!r}")

    raise TypeError(f"unexpected value for {key!r}")
