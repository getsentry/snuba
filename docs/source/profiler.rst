Profiling
=========

Snuba uses `Sentry's own profiling
<https://docs.sentry.io/product/profiling/>`_ to capture profiles as part of
regular tracing.

Only enabled for a few deployments selectively via environment variables. Snuba
admin is sampled at 100%, and some low-scale consumers also have a sample rate
set via ``SNUBA_PROFILES_SAMPLE_RATE``.
