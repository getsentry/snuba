Profiling
=========

Snuba has two ways of using `Sentry's own profiling
<https://docs.sentry.io/product/profiling/>`_: Profiles captured as part of
regular transactions like API calls, and "on-demand" profiles.

Regular transaction profiling
=============================

Only enabled for a few deployments selectively via environment variables. Snuba
admin is sampled at 100%, and some low-scale consumers also have a sample rate
set.

On-demand profiling
===================

For consumers who have ``SNUBA_PROFILES_SAMPLE_RATE`` set to a nonzero value,
it is possible to capture a profile of the main thread for a fixed duration of
time via runtime config.

For example, if the deployment ``snuba-querylog`` misbehaves, you can pick a
specific pod by ``pod_name`` like
``snuba-querylog-consumer-production-6d95f9c8d9-4cqht`` and enable profiling
just for that one pod.

Since profiling has unknown amount of overhead, pick only a few pods at a time
to minimize the risk and size of a backlog.

Once you have that ``pod_name``, set the runtime config
``ondemand_profiler_hostnames`` to that value, without quotes. Separate
multiple values by comma.

To stop profiling and send the profile to Sentry, unset the value.

You should see a warning message in Sentry saying "Starting ondemand profile
for <hostname>", and once you unset runtime config, a new profile with a
transaction name of "ondemand profile: <hostname>"

Profiles are capped to 30 second runtime, so if the runtime config is set
forever, there will be profiles sent to Sentry every 30 seconds.
