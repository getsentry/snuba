from contextlib import AbstractContextManager
from typing import Any

from sentry_options.testing import override_options

from snuba.configs.configuration import (
    CONFIGURABLE_COMPONENT_OBJECT_OVERRIDES_KEY,
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

    The override bags are single dict options, so overriding multiple keys must
    happen in one call — nesting ``override_component_config`` contexts would
    replace a whole dict each time, hiding all but the innermost key. Each entry
    is ``(component, config_key, value[, params])``. Object-typed configs
    (``value_type`` == ``dict``) are routed to ``configurable_component_object_overrides``
    and numeric configs to ``configurable_component_overrides``, matching how
    ``get_config_value`` reads them.
    """
    numeric: dict[str, Any] = {}
    objects: dict[str, Any] = {}
    for component, config_key, value, *rest in overrides:
        params = rest[0] if rest else None
        full_key = component._build_config_key(config_key, params or {})
        if component.config_definitions()[config_key].value_type is dict:
            objects[full_key] = value
        else:
            numeric[full_key] = value
    option_overrides: dict[str, Any] = {}
    if numeric:
        option_overrides[CONFIGURABLE_COMPONENT_OVERRIDES_KEY] = numeric
    if objects:
        option_overrides[CONFIGURABLE_COMPONENT_OBJECT_OVERRIDES_KEY] = objects
    return override_options("snuba", option_overrides)


__all__ = ["override_component_config", "override_component_configs"]
