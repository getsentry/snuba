# Setting allocation-policy config overrides

Allocation-policy (and other `ConfigurableComponent`) config values are read from
**sentry-options** and managed centrally in **sentry-options-automator**. When an
override is absent the code default applies; the legacy Redis runtime config is no
longer consulted for reads.

The policies themselves are unchanged — only *where* a config value comes from has
moved (Redis runtime config → sentry-options). Keys, parameters, and semantics are
exactly as before.

All configs live in a single option, a flat `{key: number}` map:

| Option | Value shape |
| --- | --- |
| `configurable_component_overrides` | `{ "<key>": number }` |

## The key

```
{resource}.{ClassName}.{config}
{resource}.{ClassName}.{config}|{param}:{value}|...   # parameterized config
```

- **resource** — the storage/strategy the component is attached to, e.g. `errors`, `eap_items`.
- **ClassName** — the concrete class, e.g. `BytesScannedRejectingPolicy`,
  `ConcurrentRateLimitAllocationPolicy`.
- **config** — the config name, e.g. `is_enforced`, `max_threads`, `concurrent_limit`.
- **params** — for a parameterized config, its declared params appended as a
  sorted, `|`-delimited `param:value` suffix, e.g.
  `|organization_id:123` or `|organization_id:123|referrer:api.foo`.

The lookup is an **exact match** on this key (the same key the policy has always
built); there is no fallback/precedence walk in the base — a policy reads exactly
the key it asks for, or the code default. A config's parameters are whatever it
declares in `param_types`.

## Example

```json
{
  "errors.BytesScannedRejectingPolicy.is_enforced": 1,
  "errors.ConcurrentRateLimitAllocationPolicy.concurrent_limit": 22,
  "errors.BytesScannedRejectingPolicy.organization_referrer_scan_limit|organization_id:456": 20000000000
}
```

## Notes

- The value is cast to the config's declared `int`/`float` type on read.
- Values must be numeric — all current `ConfigurableComponent` configs are.
- Parameterized (optional) configs are keyed exactly as the policy builds them;
  snuba-admin's Capacity Management view lists the live configs and their params.

## Finding the exact names

- **resource** — the `StorageKey`/resource value the component is registered on.
- **ClassName** — the class name.
- **config** — the `Configuration` entries in the component's
  `_additional_config_definitions` (plus the base `is_active` / `is_enforced` /
  `max_threads`), with their `param_types` giving the params.
