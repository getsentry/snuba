from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sentry_options.testing import override_options

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


def _validated_entry(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    params: dict[str, Any],
) -> tuple[str, Any]:
    """The (full key, cast value) an override should store, validated like the
    config definition."""
    definition = component._validate_config_params(config_key, params, value)
    return component._build_config_key(config_key, params), definition.value_type(value)


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
    full_key, cast_value = _validated_entry(component, config_key, value, params or {})
    bag = _current_bag()
    bag[full_key] = cast_value
    _write_bag(bag)


@contextmanager
def override_component_config(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    params: dict[str, Any] | None = None,
) -> Iterator[None]:
    """Context-manager form of ``set_component_config``: sets the override for the
    duration of the block and restores the prior bag on exit (via the public
    ``override_options`` API). Merges into any existing overrides rather than
    replacing them."""
    full_key, cast_value = _validated_entry(component, config_key, value, params or {})
    with override_options(
        "snuba", {CONFIGURABLE_COMPONENT_OVERRIDES_KEY: {**_current_bag(), full_key: cast_value}}
    ):
        yield


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
