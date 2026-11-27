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
- **ClassName** — the concrete class, e.g. `ConcurrentQueriesPerOrgPolicy`.
- **config** — the config name, e.g. `is_enforced`, `max_threads`, `concurrent_limit`.
- **params** — for a parameterized config, its declared params appended as a
  sorted, `|`-delimited `param:value` suffix, e.g.
  `|organization_id:123` or `|organization_id:123|referrer:api.foo`.

The lookup is an **exact match** on this key (the same key the policy has always
built); there is no fallback/precedence walk in the base — a policy reads exactly
the key it asks for, or the code default. A config's parameters are whatever it
declares in `param_types`.

## Example

Which allocation policies run on a storage, and their settings, are set in the
`allocation_policies` option. Each storage maps to a list of blocks. A block
applies when, for every `match` key, the query's tenant id is one of the listed
values (`match: {}` always applies). Blocks apply in order: `remove` drops policies by name, then
`policies` adds or replaces them by name.

```yaml
allocation_policies:
  errors:
    - match: {}
      policies:
        - name: ConcurrentQueriesPerOrgPolicy
          enforce: always
          limit: 25
          max_threads: 16
    - match:
        organization_id: [123]
      policies:
        - name: ConcurrentQueriesPerOrgPolicy
          enforce: always
          limit: 50
          max_threads: 16
    - match:
        referrer: [api.foo]
      remove: [ConcurrentQueriesPerOrgPolicy]
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
