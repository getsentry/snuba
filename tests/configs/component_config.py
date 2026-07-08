from contextlib import AbstractContextManager
from typing import Any

from sentry_options.testing import override_options

from snuba.configs.configuration import (
    CONFIGURABLE_COMPONENT_OVERRIDES_KEY,
    ConfigurableComponent,
)

# A single override entry: (component, config_key, value[, params]).
ComponentConfigOverride = (
    tuple[ConfigurableComponent, str, Any]
    | tuple[ConfigurableComponent, str, Any, dict[str, Any] | None]
)


def override_component_config(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    params: dict[str, Any] | None = None,
) -> AbstractContextManager[None]:
    """Set a ConfigurableComponent config via the ``configurable_component_overrides``
    sentry-option for the duration of the context.

    ConfigurableComponents (allocation policies and routing strategies alike) no
    longer fall back to the legacy Redis runtime config, so ``set_config_value``
    writes are not read back by ``get_config_value``; tests must supply overrides
    through this option instead. The key is built with the component's own key
    builder so it matches exactly what ``get_config_value`` looks up, and the
    value is stored as a number just like production data.
    """
    return override_component_configs((component, config_key, value, params))


def override_component_configs(
    *overrides: ComponentConfigOverride,
) -> AbstractContextManager[None]:
    """Set several ConfigurableComponent configs at once.

    ``configurable_component_overrides`` is a single dict option, so overriding
    multiple keys must happen in one call — nesting ``override_component_config``
    contexts would replace the whole dict each time, hiding all but the innermost
    key. Each entry is ``(component, config_key, value[, params])``.
    """
    merged: dict[str, Any] = {}
    for component, config_key, value, *rest in overrides:
        params = rest[0] if rest else None
        full_key = component._build_runtime_config_key(config_key, params or {})
        merged[full_key] = value
    return override_options("snuba", {CONFIGURABLE_COMPONENT_OVERRIDES_KEY: merged})


__all__ = ["override_component_config", "override_component_configs"]
