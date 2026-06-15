"""Unit tests for the EAP downsample materialized view introduced in
migration 0057_sample_downsample_tiers_by_trace.

These check:
- the new MV samples per item via a single, un-perturbed hash of `item_id`
  (so inclusion stays independent across items and the extrapolation
  variance math keeps its Bernoulli-independence assumption), and
- because the sampling weights are 8 / 64 / 512 (each divides the next) and
  every tier hashes the same value, any hash value that satisfies
  `H % 512 == 0` also satisfies `H % 64 == 0` and `H % 8 == 0` — i.e.
  tier 512 ⊆ tier 64 ⊆ tier 8.
"""

from importlib import import_module

_migration = import_module(
    "snuba.snuba_migrations.events_analytics_platform.0057_sample_downsample_tiers_by_trace"
)


def test_new_mv_samples_per_item_without_perturbation() -> None:
    for sampling_weight in _migration.sampling_weights:
        sql = _migration.generate_new_materialized_view_expression(sampling_weight)
        # Hash `item_id` directly (no `+ weight` perturbation) so every tier
        # hashes the same value and the tiers nest as subsets.
        assert f"cityHash64(item_id) % {sampling_weight}" in sql, (
            f"sampling weight {sampling_weight} should hash item_id unperturbed, got: {sql}"
        )
        # The perturbed per-tier form must be gone — otherwise the tiers are
        # not subsets of each other.
        assert f"item_id + {sampling_weight}" not in sql
        # Sampling must stay per-item, not per-trace, to preserve the
        # independence assumption in the extrapolation variance math.
        # (`trace_id` still appears as a passed-through SELECT column, so we
        # only assert it is not the hashed/sampled value.)
        assert "reinterpretAsUInt128(trace_id)" not in sql
        assert "cityHash64(trace_id)" not in sql


def test_subset_property_via_modular_arithmetic() -> None:
    # The MV uses `WHERE cityHash64(item_id) % w = 0`.
    # If H % 512 == 0 then H % 64 == 0 and H % 8 == 0, since 8 | 64 | 512.
    # Verify the divisibility chain that the SQL relies on for the subset
    # guarantee.
    weights = sorted(_migration.sampling_weights)
    for smaller, larger in zip(weights, weights[1:]):
        assert larger % smaller == 0, (
            f"sampling_weight {larger} must be a multiple of {smaller} "
            "for tier subset property to hold"
        )

    # Spot-check with concrete hash values. Any H that lands in the tightest
    # tier (largest weight) must also land in every looser tier.
    largest = max(_migration.sampling_weights)
    for k in range(10):
        h = k * largest  # H % largest == 0 by construction
        for w in _migration.sampling_weights:
            assert h % w == 0, (
                f"hash {h} passes tier {largest} but not tier {w}; subset property violated"
            )
