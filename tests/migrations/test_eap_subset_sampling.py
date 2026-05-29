"""Unit tests for the EAP downsample materialized view introduced in
migration 0055_sample_downsample_tiers_by_trace.

These check:
- the new MV samples on `trace_id` (so every item in a trace lands in the
  same set of tiers), and
- because the sampling weights are 8 / 64 / 512 (each divides the next),
  any hash value that satisfies `H % 512 == 0` also satisfies
  `H % 64 == 0` and `H % 8 == 0` — i.e. tier 512 ⊆ tier 64 ⊆ tier 8.
"""

from importlib import import_module

_migration = import_module(
    "snuba.snuba_migrations.events_analytics_platform.0055_sample_downsample_tiers_by_trace"
)


def test_new_mv_samples_on_trace_id() -> None:
    for sampling_weight in _migration.sampling_weights:
        sql = _migration.generate_new_materialized_view_expression(sampling_weight)
        assert f"cityHash64(reinterpretAsUInt128(trace_id)) % {sampling_weight}" in sql, (
            f"sampling weight {sampling_weight} should hash trace_id, got: {sql}"
        )
        # The previous per-item-id form must be gone — otherwise items in
        # the same trace can land in different tiers.
        assert "cityHash64(item_id" not in sql


def test_subset_property_via_modular_arithmetic() -> None:
    # The MV uses `WHERE cityHash64(reinterpretAsUInt128(trace_id)) % w = 0`.
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
