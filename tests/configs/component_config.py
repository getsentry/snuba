from typing import Any

from snuba.configs.configuration import (
    CONFIGURABLE_COMPONENT_OVERRIDES_KEY,
    ConfigurableComponent,
)
from snuba.state.sentry_options import get_option

# Config values for ConfigurableComponents are read from the
# ``configurable_component_overrides`` sentry-option (managed in
# sentry-options-automator), not from Redis, and there is no runtime write API.
# Tests set overrides directly in that option through these helpers; the autouse
# fixture in tests/conftest.py clears the option between tests.


def _current_bag() -> dict[str, Any]:
    current: Any = get_option(CONFIGURABLE_COMPONENT_OVERRIDES_KEY, {})
    return dict(current) if isinstance(current, dict) else {}


def _write_bag(bag: dict[str, Any]) -> None:
    from sentry_options._core import _set_override

    _set_override("snuba", CONFIGURABLE_COMPONENT_OVERRIDES_KEY, bag)


def set_raw_component_overrides(bag: dict[str, Any]) -> None:
    """Replace the whole override bag verbatim (no key building / validation).

    For tests that need to inject exactly-shaped keys, e.g. malformed ones.
    """
    _write_bag(bag)


def clear_component_config_overrides() -> None:
    """Drop all component-config overrides (used by the autouse conftest fixture)."""
    from sentry_options._core import _clear_override

    _clear_override("snuba", CONFIGURABLE_COMPONENT_OVERRIDES_KEY)


def set_component_config(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    params: dict[str, Any] | None = None,
) -> None:
    """Set a ConfigurableComponent config override in the sentry-option that
    ``get_config_value`` reads. Validates + casts like the config definition."""
    params = params or {}
    definition = component._validate_config_params(config_key, params, value)
    full_key = component._build_config_key(config_key, params)
    bag = _current_bag()
    bag[full_key] = definition.value_type(value)
    _write_bag(bag)


def delete_component_config(
    component: ConfigurableComponent,
    config_key: str,
    params: dict[str, Any] | None = None,
) -> None:
    """Remove a ConfigurableComponent config override from the sentry-option."""
    params = params or {}
    component._validate_config_params(config_key, params)
    full_key = component._build_config_key(config_key, params)
    bag = _current_bag()
    bag.pop(full_key, None)
    _write_bag(bag)
