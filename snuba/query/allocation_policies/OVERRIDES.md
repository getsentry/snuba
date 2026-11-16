# Configuring allocation policies

Allocation policies are configured only through the `allocation_policies`
sentry-option, managed in **sentry-options-automator**. `resolver.py` reads it on
every query; the schema is in `sentry-options/schemas/snuba/schema.json`.

## Shape

```yaml
allocation_policies:
  <resource>:        # storage key for SnQL (e.g. errors), EAP for RPC routing
    - match: {}      # a match block
      remove: [...]  # optional
      policies: [...]
      thresholds: {} # optional
```

Blocks whose `match` fits the query's tenant ids are applied in file order:

- `match` keys are `organization_id`, `project_id` and `referrer`, each a list.
  All keys must match (AND), any value in a list matches (OR). `match: {}`
  matches every query.
- `remove` drops policies added by earlier blocks, by name.
- `policies` adds a policy, or replaces an earlier one with the same name.
- `thresholds.reject` holds load ceilings keyed by `LoadInfo` field. They are
  copied onto every policy in the block and decide when `enforce: pardon` lets a
  rejected query run.

An unknown policy name is skipped with a warning. If no policy is left, the
resource gets a `PassthroughPolicy`.

## Policy items

`name` picks the class. Every other key is passed to its constructor, so an
unknown key raises a `TypeError` when the policy is built.

| Key | Policies | Default | Meaning |
| --- | --- | --- | --- |
| `enforce` | all | `always` | `always` rejects; `pardon` rejects unless load is under `thresholds.reject`; `never` only records |
| `max_threads` | all | 10 | Threads a query runs with when allowed |
| `limit` | `BytesScannedPer*` | 1280000000000 | Bytes allowed per key in the window |
| `time_window_minutes` | `BytesScannedPer*` | 10 | Sliding window length |
| `timeout_penalty` | `BytesScannedPer*` | 0.025 | Share of `limit` counted for each timed-out query |
| `limit` | `ConcurrentQueriesPer*` | 22 | Queries allowed to run at once per key |

`Per*` is the counting key: `PerOrgPolicy` (organization), `PerRefPolicy`
(referrer) or `PerOrgRefPolicy` (organization and referrer).

## Example

```yaml
allocation_policies:
  errors:
    - match: {}
      policies:
        - name: BytesScannedPerOrgRefPolicy
          enforce: pardon
        - name: ConcurrentQueriesPerOrgPolicy
          limit: 22
      thresholds:
        reject:
          cluster_load: 90
    - match:
        referrer: [subscriptions_executor]
      remove: [BytesScannedPerOrgRefPolicy]
```

Storage-routing strategy configs are separate: they are still read from the
`configurable_component_overrides` option, keyed `{resource}.{ClassName}.{config}`.
