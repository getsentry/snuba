from contextlib import AbstractContextManager
from typing import Any

from sentry_options.testing import override_options

from snuba.configs.configuration import (
    CONFIGURABLE_COMPONENT_OVERRIDES_KEY,
    SCOPED_OVERRIDE_WILDCARD,
    ConfigurableComponent,
)

# A single override entry: (component, config_key, value[, tenant_ids]).
ComponentConfigOverride = (
    tuple[ConfigurableComponent, str, Any]
    | tuple[ConfigurableComponent, str, Any, dict[str, Any] | None]
)


def override_component_config(
    component: ConfigurableComponent,
    config_key: str,
    value: Any,
    tenant_ids: dict[str, Any] | None = None,
) -> AbstractContextManager[None]:
    """Set a ConfigurableComponent config via the ``configurable_component_overrides``
    sentry-option for the duration of the context.

    Configs are read from sentry-options (not the legacy Redis runtime config), so
    tests set them through this option. ``tenant_ids`` scopes the value (empty = the
    global ``("*", "*")`` scope); it's stored in the nested
    ``{id (or "*"): {referrer (or "*"): value}}`` shape ``get_config_value`` reads.
    """
    return override_component_configs((component, config_key, value, tenant_ids))


def override_component_configs(
    *overrides: ComponentConfigOverride,
) -> AbstractContextManager[None]:
    """Set several ConfigurableComponent configs at once.

    The override bag is a single dict option, so overriding multiple keys must
    happen in one call. Each entry is ``(component, config_key, value[, tenant_ids])``;
    ``tenant_ids`` scopes the value (project_id/organization_id + referrer).
    """
    bag: dict[str, Any] = {}
    for component, config_key, value, *rest in overrides:
        tenant_ids = (rest[0] if rest else None) or {}
        full_key = component._build_config_key(config_key)
        scope_id = tenant_ids.get("project_id", tenant_ids.get("organization_id"))
        outer = str(scope_id) if scope_id is not None else SCOPED_OVERRIDE_WILDCARD
        inner = tenant_ids.get("referrer", SCOPED_OVERRIDE_WILDCARD)
        bag.setdefault(full_key, {}).setdefault(outer, {})[inner] = value
    return override_options("snuba", {CONFIGURABLE_COMPONENT_OVERRIDES_KEY: bag})


__all__ = ["override_component_config", "override_component_configs"]
