// static descriptions complementary to dynamic descriptions.
// write static descriptions here to explain what the option does (and once, for all regions/deployments)
// write dynamic descriptions in snuba-admin to explain why the option is set the way it is.
//
// due to the historically dynamic nature of runtime config,
// descriptions here can be defined to apply to an arbitrary
// *regex pattern* of description keys.
const DESCRIPTIONS: { [key: string]: string } = {
  'quantized_rebalance_consumer_group_delay_secs__.*': "quantized rebalancing means that during deploys, we try to trigger rebalancing across all pods within a consumer group at the same time. a value of 15 means that pods align their group join/leave activity to some multiple of 15 seconds, by sleeping during startup or shutdown. the setting is suffixed with __foo e.g. meaning the 'foo' consumer group is affected. this refers to de-facto consumer groups, not logical ones. the same concept exists in sentry's python consumers. in snuba, this only affects rust consumers.",
  "enable_bypass_cache_referrers": "Any referrer listed in BYPASS_CACHE_REFERRERS under Snuba settings will bypass readthrough cache",
  ".*_ignore_consistent_queries_sample_rate": "Ignore consistent queries to given dataset. This flag should be set to a value between 0 and 1 where 0 means we never ignore any incoming consistent queries and 1 means ignoring all consistent queries.",
  "generic_metrics_use_case_killswitch": "Add a use case to this list to killswitch its data coming into all generic metrics Snuba consumers. e.g. if you want to turn off the custom use case, this will make sure none of its data gets processed by any Snuba consumer",
  "http_batch_join_timeout": "PYTHON ONLY: Time in seconds to wait for clickhouse write to complete for consumers. Higher number to start off with.",
  "ondemand_profiler_hostnames": "This enables profiling for the specified host names ",
  "optimize_parallel_threads": "number of threads to run the optimize cron job with",
  "project_quota_time_percentage": "Controls the project quota limit. A counter class tracks the processing time spent on some task for a project and compares it with this quota",
  "rate_limit_shard_factor": "How many keys the query rate limiter should shard a set into. More keys means smaller avg redis-set size (therefore faster ops), but more (pipelined) ops. This would be more useful if the rate limiter redis was actually a multi-node redis cluster. Right now we run this code just so it is ready, should we have to scale, and to be able to tweak set size if we have to.",
  "read_through_cache.short_circuit": "First stage of removing the readthrough cache entirely is disabling and monitoring - Rahul",
  "retry_duplicate_query_id": "Whether to retry clickhouse queries with a random query id (exactly once) if clickhouse rejected the query before due to the query id already being used. This can be useful in case of redis failover scenarios when we lose query cache.",
  "snuba_api_cogs_probability": "Sample rate for logging COGS in the API",
}

function getDescription(key: string): [string, string] | undefined {
  if (DESCRIPTIONS[key]) return [key, DESCRIPTIONS[key]];
  for (const defKey in DESCRIPTIONS) {
    if (new RegExp(`^${defKey}$`, 'g').test(key)) {
      return [defKey, `for pattern ${defKey}: ${DESCRIPTIONS[defKey]}`];
    }
  }
}

export { getDescription };
