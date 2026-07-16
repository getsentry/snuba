# Setting allocation-policy config overrides

Allocation-policy (and other `ConfigurableComponent`) config values are read from
**sentry-options** and managed centrally in **sentry-options-automator**. When an
override is absent the code default applies; the legacy Redis runtime config is no
longer consulted.

Every config lives in a single option, keyed by a fully-qualified config key:

| Option | Value shape |
| --- | --- |
| `configurable_component_overrides` | `{ "<key>": { id (or "*"): { referrer (or "*"): number } } }` |

## The config key

```
{resource}.{ClassName}.{config}
```

- **resource** — the storage/strategy the component is attached to, e.g. `errors`, `eap_items`.
- **ClassName** — the concrete class, e.g. `BytesScannedRejectingPolicy`,
  `ConcurrentRateLimitAllocationPolicy`.
- **config** — the config name, e.g. `is_enforced`, `max_threads`, `concurrent_limit`.

There are no parameters in the key — **the scoping lives inside the value**.

## Scoping: global / per-org / per-referrer / per-(org, referrer)

Every config's value is a two-level nested object
`{ id (or "*"): { referrer (or "*"): value } }`, where `id` is a
project_id/organization_id. This lets one config carry, at once:

- a **global** value — `{"*": {"*": v}}`
- a **per-id** value (project/org) — `{"<id>": {"*": v}}`
- a **per-referrer** value — `{"*": {"<referrer>": v}}`
- a **per-(id, referrer)** value — `{"<id>": {"<referrer>": v}}`

On read, the value is resolved for the query's tenants **most-specific-first**, and
the first match wins:

```
(id, referrer)  >  (id, "*")  >  ("*", referrer)  >  ("*", "*")  >  code default
```

`id` is the query's `project_id` if present, otherwise its `organization_id`.
Ids are stringified (`"123"`); referrers keep their dots (`"api.foo"`).

## Example

```json
{
  "errors.ConcurrentRateLimitAllocationPolicy.concurrent_limit": {
    "123": { "api.foo": 4, "*": 20 },
    "*":   { "api.foo": 40 }
  },
  "errors.BytesScannedRejectingPolicy.is_enforced": {
    "*": { "*": 1 }
  },
  "errors.BytesScannedRejectingPolicy.organization_referrer_scan_limit": {
    "456": { "*": 20000000000 }
  }
}
```

With the above, for a query on storage `errors`:

| Query tenants | `concurrent_limit` | Matched tier |
| --- | --- | --- |
| org `123`, referrer `api.foo` | `4` | `(id, referrer)` |
| org `123`, referrer `other` | `20` | `(id, "*")` |
| org `999`, referrer `api.foo` | `40` | `("*", referrer)` |
| org `999`, referrer `other` | code default (`22`) | none |

## Notes

- **Any config is scopable**, including the base toggles `is_active` /
  `is_enforced` / `max_threads` — a per-org `is_enforced` or `max_threads` takes
  effect for that org's queries.
- The value is cast to the config's declared `int`/`float` type on read.
- Resolution is **most-specific-wins** (precedence), applied uniformly to every
  policy — including `ConcurrentRateLimitAllocationPolicy`, which previously used a
  min-of-all-applicable rule.

## Finding the exact names

- **resource** — the `StorageKey`/resource value the component is registered on.
- **ClassName** — the class name.
- **config** — the `Configuration` entries in the component's
  `_additional_config_definitions` (plus the base `is_active` / `is_enforced` /
  `max_threads`). snuba-admin's Capacity Management view also lists the live
  configs for a policy.
