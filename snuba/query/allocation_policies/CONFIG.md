# Configuring allocation policies

Allocation policies are configured in two sentry-options, managed in
sentry-options-automator:

| Option | Set in | Purpose |
| --- | --- | --- |
| `allocation_policies.base` | `default` target only | The baseline for every region |
| `allocation_policies.region` | region targets | Only what differs in that region |

snuba deep-merges `region` over `base`: maps merge key by key, `overrides`
lists are appended after the base ones, any other value is replaced. Region
files therefore never have to copy the baseline.

A resource listed under `resources` here ignores the legacy
`allocation_policies` option. Resources that aren't listed still use it.

## Shape

```yaml
allocation_policies.base:
  defaults:                     # inherited by every policy of every resource
    max_threads: 16             # enforce is left out: it defaults to true
  resources:
    errors:                     # storage key, or EAP for RPC routing
      enforce: false            # measure only; inherited by every policy here
      policies:
        bytes_scanned_rejecting: {}
        referrer_guard_rail: {}
    transactions:
      thresholds:               # pardon ceilings, inherited like enforce
        reject: { cluster_load: 0.5 }
      policies:                 # read queries
        bytes_scanned_window:
          org_bytes_limit: 400000000000
          throttled_max_threads: 4
        concurrent_rate_limit:
          concurrent_limit: 20
        referrer_guard_rail:
          concurrent_limit: 40
      delete_policies:          # delete queries
        delete_concurrent_rate_limit:
          concurrent_limit: 2
      overrides:
        - match: { referrer: [subscriptions_executor] }
          policies:
            referrer_guard_rail: { concurrent_limit: 100 }
        - match: { project_id: [5868616] }
          policies:
            bytes_scanned_rejecting: { project_bytes_limit: 200000000000 }
```

## How a value is resolved

For one query, most specific first:

1. Matching `overrides`. A `match` checks `organization_id`, `project_id` and
   `referrer` (AND across keys, OR within a list). When several match, the one
   with more matched keys wins; on a tie, the later one wins (region after base).
2. The policy's entry under `policies` / `delete_policies`.
3. The resource's `enforce`, `pardonable`, `max_threads`, `thresholds`.
4. `defaults`.
5. The code default.

An override changes only the fields it lists. It can add a policy that the
resource doesn't have, and `enabled: false` removes one.

A policy that limits delete queries is ignored under `policies`, and one that
limits read queries is ignored under `delete_policies`.

## Policies and settings

Every policy accepts `enforce`, `pardonable` (booleans), `max_threads` and
`enabled`.

| Policy key | Class | Settings |
| --- | --- | --- |
| `bytes_scanned_rejecting` | `BytesScannedRejectingPolicy` | `project_bytes_limit`, `org_bytes_limit`, `max_bytes_to_read`, `cap_instead_of_reject`, `timeout_penalty_bytes`, `throttle_bytes_divider`, `throttle_threads_divider`, `max_bytes_to_read_divider` |
| `bytes_scanned_window` | `BytesScannedWindowAllocationPolicy` | `org_bytes_limit`, `throttled_max_threads` |
| `concurrent_rate_limit` | `ConcurrentRateLimitAllocationPolicy` | `concurrent_limit`, `rate_limit_shard_factor`, `max_query_duration_s` |
| `delete_concurrent_rate_limit` | `DeleteConcurrentRateLimitAllocationPolicy` | same as `concurrent_rate_limit` |
| `cross_org` | `CrossOrgQueryAllocationPolicy` | `referrer_limits`, `rate_limit_shard_factor`, `max_query_duration_s` |
| `referrer_guard_rail` | `ReferrerGuardRailPolicy` | `concurrent_limit`, `throttle_requests_divider`, `throttle_threads_divider`, `rate_limit_shard_factor`, `max_query_duration_s` |

An unknown policy key or setting is logged and ignored. The mapping to each
class's config names is `POLICY_SETTINGS` in `policy_config.py`.

Per-tenant numbers go in `overrides`, not in parameterized config keys such as
`organization_override|organization_id:1`.
