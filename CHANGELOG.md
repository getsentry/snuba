# Changelog

## 22.2.0

### Various fixes & improvements

- fix(admin): Fix the system query tool (#2414) by @lynnagara
- ref: Clean up snql config use in tests (#2412) by @lynnagara
- ref: Remove use of deprecated config in test (#2411) by @lynnagara
- ref: Don't set query_parsing_expand_aliases in tests (#2413) by @lynnagara
- perf(subscriptions): Increase scheduler load factor to 5 (#2406) by @lynnagara
- feat(stability) - fallback host retry (#2398) by @onewland
- Revert "feat(errors): Add a new errors_v2 storage option (#2349)" (#2404) by @nikhars
- perf(subscriptions): Increase the load factor to 3 (#2401) by @lynnagara
- ref(subscriptions): Remove unnecessary assertion (#2402) by @lynnagara
- feat(errors): Add a new errors_v2 storage option (#2349) by @nikhars
- perf(subscriptions): Avoid slow delegate task builder where possible (#2399) by @lynnagara
- fix(cleanup) Fix rounding error in cleanup script (#2394) by @evanh
- fix(conn) increase retry count by 1 in ClickhousePool.execute (#2397) by @onewland
- feat(ingestion): Allow for failures during writes (#2395) by @nikhars
- ref(snuba-admin): Edit predefined queries (#2382) by @MeredithAnya
- feat(consumer-util): Expose metric of consumer utilization (#2245) by @nikhars
- ref(attribution) Add test hook for metrics (#2391) by @evanh
- ref: Add outcomes-billing-consumer to freight (#2384) by @evanh
- fix(consumer): Allow commit log topic to be passed to multistorage consumer (#2393) by @nikhars
- feat(metrics): directly write aggregates to metrics table (#2361) by @onewland
- docs(internal): Add some doc comments for MetricsBackend (#2389) by @onewland
- chore(kafka): Add a log message of librdkafka log level (#2208) by @nikhars
- feat(dev): Mock data for subscriptions (#2390) by @lynnagara
- Fix metrics init (#2392) by @fpacifici

_Plus 40 more_

## 22.1.0

### Various fixes & improvements

- Enforce uniq in HAVING and SELECT (#2339) by @vladkluev
- feat(subscriptions): Update command args to accept metrics entities (#2302) by @ahmedetefy
- feat(tracing) Add a backend for trace data (#2309) by @evanh
- feat(snql) Support higher order functions in SnQL (#2333) by @evanh
- Update ci.yml (#2338) by @vladkluev
- Fix the shutdown process of the scheduler (#2332) by @fpacifici
- Revert accidental push to master, disallow this from happening again (#2336) by @vladkluev
- feat(ops): add transactions_ro storage support (#2331) by @onewland
- feat(config): prepare for readonly transactions storage (#2334) by @onewland
- add as code formatter to test utils (3a7bb64d)
- use https instead of git (#2335) by @evanh
- feat(subscriptions): Adds metrics topics to topics validation (#2326) by @ahmedetefy
- feat(subscriptions): Integrate ProduceResult step into executor (#2299) by @lynnagara
- Roll out redundant tag optimization to all queries (#2330) by @vladkluev
- fix(admin): Don't log invalid system table or column errors (#2321) by @lynnagara
- feat(subscriptions): Only execute scheduled tasks that match entity (#2298) by @lynnagara
- feat(admin) Add authorization provider in snuba admin (#2301) by @fpacifici
- fix(arm64) Fix tests so they work on arm64 as well (#2329) by @evanh
- feat(admin): Improve admin notifications (#2328) by @lynnagara
- Fix redundant clause optimizer not capturing experiment data (#2327) by @vladkluev
- fix(cra-metrics): Send `entity` in subscription payload (#2291) by @ahmedetefy
- Revert "feat(freight): Deploy subscriptions executor with Freight (#2322)" (#2323) by @lynnagara
- feat(freight): Deploy subscriptions executor with Freight (#2322) by @lynnagara
- feat: Commit offsets on subscription executor if sample rate is set to 0 (#2317) by @lynnagara
- fix: Prevent all subscription from being logged as non-matching (#2315) by @lynnagara
- feat(admin): Allow % in extra part of system query (#2320) by @lynnagara
- Render system query results in a table (#2319) by @vladkluev
- add retries to redis initialization (#2318) by @vladkluev
- Revert "make the query results render in a table" (#2316) by @vladkluev
- make the query results render in a table (9d80e851)
- Relax bigquery querylog anonymization (#2308) by @vladkluev
- feat(admin): Better word wrapping for runtime config (#2312) by @lynnagara
- feat(admin): Send SQL queries from the UI to system tables (#2311) by @lynnagara
- Revert "feat(metric-alerts): Adds subscription consumer for metrics [INGEST-623] (#2222)" (#2314) by @ahmedetefy
- fix(pipeline): Remove capture trace flag from Request (#2305) by @rahul-kumar-saini
- Revert "feat(subscriptions): Adds metrics topics to topics validation (#2303)" (#2313) by @ahmedetefy
- feat(subscriptions): Adds metrics topics to topics validation (#2303) by @ahmedetefy
- feat(metric-alerts): Adds subscription consumer for metrics [INGEST-623] (#2222) by @ahmedetefy
- feat(admin): Replace nested table with raw JSON and copy to clipboard button (#2310) by @lynnagara
- Add documentation on how to run admin portal (#2306) by @vladkluev
- validate by node list (#2307) by @onewland
- feat(tracing) Capture trace output in clickhouse result (#2304) by @evanh
- feat(admin): Add ability to edit existing runtime config (#2297) by @lynnagara
- feat(admin): Extend /run_clickhouse_system_query to support custom SQL queries (#2296) by @lynnagara
- feat(admin) - wire in queries and rendering results (#2294) by @onewland
- feat(config): check value types (#2187) by @nikhars
- feat(pipeline): Capture Trace flag (#2293) by @rahul-kumar-saini
- feat(admin): Automatically log to Slack if configuration is provided (#2290) by @lynnagara
- ref(api): Removes deprecated routes (#2292) by @ahmedetefy
- feat(admin): Add ability to delete runtime config items (#2288) by @lynnagara
- fix(subscriptions): Ensure producer is closed on SIGINT and SIGTERM (#2289) by @lynnagara
- change confusing warning message (#2295) by @vladkluev
- feat(subscriptions): Executor uses the ExecuteQuery strategy (#2280) by @lynnagara
- feat(subscriptions): Add the strategy to produce a subscription result (#2287) by @lynnagara
- fix(parsing): Group By clause in AST is now a List (#2284) by @rahul-kumar-saini
- feat(tracing) Change the return type of the native execute  (#2279) by @evanh
- feat(cra-metrics): Adds MetricsSetsSubscription (#2276) by @ahmedetefy
- feat(admin): Runtime config notifications take two (#2285) by @lynnagara
- build(craft): Enable automatic Changelogs (#2235) by @BYK
- Revert "perf(subscriptions): Increase load factor to 3 for subscriptions load test (#2271)" (#2286) by @lynnagara
- perf(subscriptions): Increase load factor to 3 for subscriptions load test (#2271) by @lynnagara
- feat: Subscription executor has --override-result-topic option (#2281) by @lynnagara
- feat(admin): Add UI component that loads the query node data (#2283) by @lynnagara
- meta: Bump new development version (63881433)

