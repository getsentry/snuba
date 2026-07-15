# Setting allocation-policy config overrides

Allocation-policy (and other `ConfigurableComponent`) config values are read from
**sentry-options** and managed centrally in **sentry-options-automator**. When an
override is absent the code default applies; the legacy Redis runtime config is no
longer consulted.

Values live in two options, both keyed by a fully-qualified config key:

| Option | Value shape | Used for |
| --- | --- | --- |
| `configurable_component_overrides` | `{ "<key>": <number> }` | numeric (int/float) configs |
| `configurable_component_object_overrides` | `{ "<key>": { ... } }` | object (`dict`) configs, e.g. scoped overrides |

## The config key

```
{resource}.{ClassName}.{config}
```

- **resource** — the storage the policy is attached to, e.g. `errors`, `eap_items`.
- **ClassName** — the concrete policy class, e.g. `BytesScannedRejectingPolicy`,
  `ConcurrentRateLimitAllocationPolicy`.
- **config** — the config name, e.g. `is_enforced`, `max_threads`, `concurrent_limit`.

For **numeric parameterized** configs, each param is appended as `|name:value`
(sorted by name); `|` separates params and the first `:` separates a name from its
value (so values may contain `.`/`,`/`:`, and never need escaping):

```
errors.ConcurrentRateLimitAllocationPolicy.organization_override|organization_id:123
errors.ConcurrentRateLimitAllocationPolicy.referrer_organization_override|organization_id:123|referrer:api.foo
```

Object configs (below) take no params — the scoping lives inside the value.

## Numeric configs

Set the fully-qualified key to a number in `configurable_component_overrides`:

```json
{
  "errors.BytesScannedRejectingPolicy.is_enforced": 1,
  "errors.ConcurrentRateLimitAllocationPolicy.concurrent_limit": 40,
  "errors.ConcurrentRateLimitAllocationPolicy.organization_override|organization_id:123": 80
}
```

The value is cast to the config's declared `int`/`float` type on read.

## Scoped overrides (project / organization / referrer)

`BytesScannedRejectingPolicy` resolves its per-scope scan limits from two
object configs, each a nested map `{ id (or "*"): { referrer (or "*"): limit } }`:

- `project_referrer_scan_limit_overrides`
- `organization_referrer_scan_limit_overrides`

One config sets a limit for a given id, a given referrer, or both. Lookups are
**most-specific-first**, first match wins:

```
(id, referrer)  >  (id, "*")  >  ("*", referrer)  >  code default
```

where the code default is `project_referrer_scan_limit` / `organization_referrer_scan_limit`.

Example (`configurable_component_object_overrides`):

```json
{
  "errors.BytesScannedRejectingPolicy.organization_referrer_scan_limit_overrides": {
    "123": { "api.foo": 100, "*": 500 },
    "*":   { "api.foo": 1000 }
  },
  "errors.BytesScannedRejectingPolicy.project_referrer_scan_limit_overrides": {
    "4505240668733440": { "*": 20000000000 }
  }
}
```

With the above, for a query on storage `errors`:

| Query tenants | Resolved limit | Matched tier |
| --- | --- | --- |
| org `123`, referrer `api.foo` | `100` | `(id, referrer)` |
| org `123`, referrer `other` | `500` | `(id, "*")` |
| org `999`, referrer `api.foo` | `1000` | `("*", referrer)` |
| org `999`, referrer `other` | code default | none |
| project `4505240668733440`, any referrer | `20000000000` | `(id, "*")` |

Notes:
- A query keyed by `project_id` uses the project config; otherwise the
  organization config. `"*"` is the wildcard for "any id" / "any referrer".
- Keys are literal JSON strings — ids are stringified (`"123"`), and referrers
  keep their dots (`"api.foo"`).

## Finding the exact names

- **resource** — the `StorageKey` value the policy is registered on (see the
  storage YAML / `get_allocation_policies`).
- **ClassName** — the policy class name.
- **config / params** — the `Configuration` entries in the policy's
  `_additional_config_definitions` (and the base `is_active` / `is_enforced` /
  `max_threads`). snuba-admin's Capacity Management view also lists the live
  configs and their params for a policy.

## Per-policy resolution semantics

Scoped nested-object overrides with the precedence above currently apply to
`BytesScannedRejectingPolicy`. The `ConcurrentRateLimitAllocationPolicy` family
still uses individual parameterized numeric override configs
(`organization_override`, `project_override`, `referrer_organization_override`,
`referrer_project_override`) and applies the **most restrictive** (minimum) of all
that match a query, rather than first-match precedence. Consult a policy's config
definitions for exactly which overrides it supports.
