# Changelog

## 22.5.0

### Various fixes & improvements

- fix(tiger): don't call the explicit tuple function in clickhouse (#2699) by @volokluev
- fix(dlq): Metrics ignored messages should be Sentry Errors (#2698) by @rahul-kumar-saini
- fix: Allow arbitrary AppIDs (#2692) by @evanh
- dlq(metrics): Metrics produce policy + basic validation replaced with DLQ stuff (#2614) by @rahul-kumar-saini
- feat(devserver): Force latest offset in devserver (#2573) by @mitsuhiko
- fix(querylog): fix the kafka config, add test for producer (#2695) by @volokluev
- fix kafka config option (#2694) by @volokluev
- fix(querylog): compress, allow larger messages on the querylog producer (#2682) by @volokluev
- ref: try building the arm64 image using linux builders (#2688) by @asottile-sentry
- fix(tiger): Implement tupleElement function in snuba (#2687) by @volokluev
- fix: Support array as well as tuple in condition (#2686) by @evanh
- feat(arroyo): Upgrade Arroyo for new Parallel Transform Step (#2679) by @rahul-kumar-saini
- feat(subscriptions): Deploy transactions scheduler and executor (#2690) by @lynnagara
- Bring in CLA Lite (#2667) by @chadwhitacre
- feat(subscriptions): Record a metric on subscription creation and deletion (#2689) by @lynnagara
- feat(subscriptions): Combined scheduler and executor (#2666) by @volokluev
- chore(tiger): Raise a different timeout exception from tiger clusters (#2680) by @nikhars
- feat(subscriptions): Remove verifier from Freight (#2684) by @lynnagara
- feat(subscriptions): Debug producer shutdown (#2685) by @lynnagara
- feat(subscriptions): Add logging for skipped subscriptions (#2678) by @lynnagara
- ref(dev-tooling): Enable commit tracking attempt II (#2677) by @MeredithAnya
- feat(subscriptions): Add config to skip stale subscriptions in executor (#2676) by @lynnagara
- Skip old subscriptions (#2675) by @fpacifici
- Add logging (#2674) by @fpacifici

_Plus 57 more_

## 22.4.0

### Various fixes & improvements

- feat(subscriptions): Temporarily run errors in global mode (#2616) by @lynnagara
- fix(subscriptions): Fix InvalidRangeError when rebalancing occurs (#2611) by @lynnagara
- fix(subscriptions): Fix off by one calculation in verifier (#2610) by @lynnagara
- feat(dlq): Introduce DLQ to Metrics with Ignore policy. (#2585) by @rahul-kumar-saini
- fix(query): Avoid race in pipeline delegator (#2608) by @nikhars
- fix: Match only migration files in scripts/ddl-changes.py (#2613) by @asottile-sentry
- feat: Bump arroyo to 0.0.16 (#2606) by @lynnagara
- feat(subscriptions): Count the off by one results separately in the verifier (#2607) by @lynnagara
- fix: Fix Clickhouse version check (#2584) by @evanh
- feat(subscriptions): Ignore stale subscriptions (#2596) by @lynnagara
- feat(subscriptions): Compare the subscription request as well in the verifier (#2600) by @lynnagara
- Revert "fix(query): Avoid race in pipeline delegator (#2597)" (#2601) by @nikhars
- fix(tiger writes): Disable ignore write errors (#2598) by @nikhars
- fix(query): Avoid race in pipeline delegator (#2597) by @nikhars
- feat: Record librdkafka buffer size from the subscriptions executor (#2599) by @lynnagara
- feat(subscriptions): Log non matching results to Sentry (#2595) by @lynnagara
- fix(subscription-verifier): Totals is not always present in result (#2594) by @lynnagara
- feat(subscriptions): Record more metrics from the subscriptions verifier (#2590) by @lynnagara
- feat(subscriptions): Record stale message topic data (#2589) by @lynnagara
- feat(admin): Added SnQL to SQL conversion (#2572) by @rahul-kumar-saini
- fix(consumers): Allow bypassing header based routing for small percentage of messages (#2233) by @nikhars
- ref(devserver): update metrics-consumer command (#2588) by @MeredithAnya
- OPS-1608: add events-subscriptions-verifier to freight config (#2592) by @mwarkentin
- fix(metrics): Remove bucket storages from devserver (#2591) by @jjbayer

_Plus 75 more_

## 22.3.0

### Various fixes & improvements

- feat(metrics) - create materialized views for polymorphic input tables (#2507) by @onewland
- Move clickhouse queries of death to public docs (#2509) by @volokluev
- feat(metrics) - create polymorphic bucket table (#2504) by @onewland
- feat(subscriptions): Deploy metrics subscriptions executor with Freight (#2501) by @lynnagara
- fix(admin): Exclude profiles from Snuba admin (#2503) by @lynnagara
- feat(profiling): Add and remove some fields (#2498) by @phacops
- chore(arroyo): Update version to record metrics (#2499) by @nikhars
- config(metrics) - add TTL to metrics tables (#2483) by @onewland
- feat: Add a default value for stats_collection_freq_ms (#2495) by @lynnagara
- ref: Update snuba-sdk version (#2485) by @evanh
- fix(subscriptions): Fix recording partition lag (#2497) by @lynnagara
- feat: Add metrics to help debug ProvideCommitStrategy (#2496) by @lynnagara
- fix(consumers): Remove deepcopy from multi storage consumers (#2491) by @nikhars
- feat(admin): Sort configs alphabetically (#2490) by @lynnagara
- feat(subscriptions): Add a metric for debugging (#2492) by @lynnagara
- feat: Deploy new metrics schedulers with Freight (#2486) by @lynnagara
- feat(subscriptions): Set executor sample rate by dataset (#2487) by @lynnagara
- perf(consumers): allow parallel collect option consumers (#2488) by @nikhars
- feat(profiling): Read and write data from and to Snuba (#2435) by @phacops
- feat(dead-letter-queue): Add dead letter queue for failed inserts (#2437) by @MeredithAnya
- test: Attempt to fix flaky test (#2482) by @lynnagara
- feat(runtime config): Add support for descriptions for configs (#2432) by @rahul-kumar-saini
- feat(alias): Add alias type that is wrapped with backticks (#2480) by @ahmedetefy
- feat(devserver): Metrics subscriptions uses new pipeline (#2479) by @lynnagara

_Plus 124 more_

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
