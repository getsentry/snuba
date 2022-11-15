# Changelog

## 22.11.0

### Various fixes & improvements

- feat(migrations): add migration order validator SNS-1831 (#3353) by @dbanda
- feat(api-abuse): Add ability to throttle threads for an entire referrer (#3376) by @volokluev
- add bytes_scanned column to querylog storage (#3381) by @volokluev
- MIGRATION: add bytes_scanned to querylog (#3360) by @volokluev
- feat(slices): Make consumers "slice-aware" (#3259) by @ayirr7
- use new checkout action to fix GHA warnings (#3377) by @volokluev
- feat(DC): More inner types for Array (#3367) by @rahul-kumar-saini
- add bytes scanned to the querylog consumer (#3375) by @volokluev
- feat(replays): Add additional parsing safety to lists and uuids (#3369) by @cmanallen
- fix(init): uWSGI Snuba API processes not initializing Snuba (#3370) by @rahul-kumar-saini
- feat(replays): Add additional parsing safety to integer and datetime column types (#3352) by @cmanallen
- Explain the ordering flags in MIGRATIONS.md (#3365) by @dbanda
- fix(admin): Allow ARRAY JOIN with clickhouse_queries in admin (#3366) by @volokluev
- ref(admin): get_migration_group_policies (#3364) by @MeredithAnya
- Pause optimize on large merges. attempt 2 (#3339) by @dbanda
- Tag keys and values may not be null (#3357) by @cmanallen
- fix(rate_limit): Set explicit TTL for set of open requests [SNS-1864] (#3362) by @untitaker
- fix(logging): Incorrect Snuba init instrumentation (#3361) by @rahul-kumar-saini
- feat(DC): Profiles Storage as Config (#3359) by @rahul-kumar-saini
- clean up project id filtering (#3355) by @volokluev
- remove logging line that is not helpful (#3356) by @volokluev
- feat(replays): Add additional safety to tags processor (#3354) by @cmanallen
- feat(api): Snuba healthcheck observability (#3334) by @rahul-kumar-saini
- add useful test distributed command to make file (#3349) by @dbanda

_Plus 68 more_

## 22.10.0

### Various fixes & improvements

- build: Actually increase the right CI timeout (#3264) by @lynnagara
- feat: Always pass a commit policy to Arroyo stream processor (#3257) by @lynnagara
- fix(gen-metrics): distributed table name wrong in config (#3260) by @onewland
- feat(TDC): Make custom function processors accessible from config (#3256) by @volokluev
- ref: Small settings cleanup (#3251) by @lynnagara
- fix(TDC) Make ConditionChecker a RegisteredClass (#3248) by @evanh
- ref(mdc): Remove dead code (#3252) by @lynnagara
- fix(JWT): stop trying to get JWT audience dynamically, hard-code in config (#3255) by @onewland
- ref: Remove filtering of error and transaction messages (#3249) by @lynnagara
- ref: Remove default mapping of "transaction" topic to "events" (#3250) by @lynnagara
- ref: Use message.position_to_commit from arroyo (#3246) by @lynnagara
- cleanup(mdc): remove flag for enabling/disabling datasets from config (#3224) by @onewland
-  ref(admin): add migration groups list endpoint (#3231) by @dbanda
- cleanup(transactions): remove transactions_ro from the codebase (#3244) by @volokluev
- fix(MDC): Incorrect column name in Gen Metrics (#3245) by @rahul-kumar-saini
- build: Arroyo 1.0.7 (#3243) by @lynnagara
- build: Increase timeout minutes in CI (#3242) by @lynnagara
- ref(admin): add migration_groups API (#3227) by @MeredithAnya
- fix: Remove filtering on offset for transactions subscriptions (#3239) by @lynnagara
- build: Remove old transaction consumer from Freight (#3237) by @lynnagara
- ref(ci): fix set-output / set-state deprecation (#3240) by @asottile-sentry
- fix(TDC) Make QuerySplitStrategy a RegisteredClass (#3238) by @evanh
- feat(transactions): app_start_type reading/writing V2 (#3209) by @philipphofmann
- fix(grouping): Group clickhouse errors by referrer (#3121) by @untitaker

_Plus 76 more_

## 22.9.0

### Various fixes & improvements

- feat(slicing): add partition mapping (#3135) by @onewland
- fix(MDC): Validate required fields and that no additional fields are added (#3137) by @rahul-kumar-saini
- fix(MDC): Fix to ensure schema is in the correct place (#3136) by @evanh
- feat(MDC): Validate configs in CI (#3128) by @rahul-kumar-saini
- fix e2e tests for dogfood self-hosted changes (#3134) by @hubertdeng123
- ref(MDC): Represent migrations in configuration (#3071) by @evanh
- feat(transactions): Add app_start_type migration (#3124) by @philipphofmann
- feat(upgrade): Similar query processors for both error storages (#3105) by @nikhars
- ref(arroyo): Fix arroyo imports (#3132) by @lynnagara
- fix(settings): Remove requirement of CDC and events to be on same cluster (#3126) by @nikhars
- feat(discover): add group_ids to discover (#3104) by @udameli
- ref(MDC): EntityKey enum to class (#3109) by @rahul-kumar-saini
- feat(mdc): Load entity subscriptions from config (#3107) by @enochtangg
- config(redis) set reinitialize_steps from settings (#3125) by @onewland
- ref(MDC): consolidate query processors into one folder (#3098) by @volokluev
- ref(EntityKey): Rename GROUPEDMESSAGES to GROUPEDMESSAGE (#3123) by @rahul-kumar-saini
- feat(MDC): Generic Metrics Dataset loaded from Config (#3108) by @rahul-kumar-saini
- ref: use internal pypi for prebuilt packages (#3100) by @asottile-sentry
- feat: Add initial documentation for Dataset configuration (#3089) by @evanh
- fix(mdc): use entity name for mapping when type is not sufficient (#3110) by @onewland
- feat(migrations): Update ddl changes script with new path to snuba_migrations (#3103) by @lynnagara
- ref(MDC): StorageKey enum to class (#3096) by @rahul-kumar-saini
- feat(mdc) add configuration for generic metrics distributions (#3102) by @onewland
- feat(mdc): Make translators, validators, and mappers registered classes (#3099) by @volokluev

_Plus 32 more_

## 22.8.0

### Various fixes & improvements

- feat: Sort raw sql fields and conditions [Experiment] (#2988) by @enochtangg
- Revert "ref(MDC): cleanup dataset factory, formalize factory pattern (#3051)" (#3052) by @volokluev
- ref(MDC): cleanup dataset factory, formalize factory pattern (#3051) by @volokluev
- feat(MDC): Add Storage yaml parsing and schema (#3046) by @rahul-kumar-saini
- config(datasets) - create a PluggableEntity class (#3050) by @onewland
- Generate abstract_column_set from replays schema (#3041) by @cmanallen
- ref: remove future (unused python2 porting library) (#3040) by @asottile-sentry
- cleanup: remove unused query processor PatternReplacer (#3038) by @onewland
- feat(replays): Add urls, user_agent, and replay_start_timestamp fields and processor (#3023) by @cmanallen
- feat(replays): Add urls, user_agent, and replay_start_timestamp migration (#3021) by @cmanallen
- feat(datasets): Add configs directory structure (#3034) by @rahul-kumar-saini
- fix(replacements): Limit size of excluded groups set [INC-190] (#3027) by @untitaker
- fix(inc): Introduce denylist that stops attempting consistent queries per project [INC-190] (#3026) by @untitaker
- Allow us to disable the global rate limiter (#3025) by @fpacifici
- build: Bump confluent sentry-python to 1.9.0 (#3022) by @andriisoldatenko
- config(ds): get rid of existing logic to move to dynamic sampling (#3018) by @onewland
- fix(discover): add missing transaction_source to TRANSACTIONS_COLUMNS (#3020) by @andriisoldatenko
- config(mep): add table rate limit and tuple unaliaser to sets/distributions (#3019) by @onewland
- Remove replays from the skipped migrations group (#3017) by @cmanallen
- feat(rollout): Make transaction query processors same (#3006) by @nikhars
- feat(replays): Enable URL processing in the consumer (#3004) by @cmanallen
- feat(replays): add error_ids column (#3001) by @JoshFerge
- fix(admin): Fix deletion of configs with `/` in them (#3016) by @nikhars
- config(mep): add schedulers/executors to snuba-stable freight (#3015) by @onewland

_Plus 56 more_

## 22.7.0

### Various fixes & improvements

- feat(mep): support time bucketing in queries (#2937) by @onewland
- add new subscription-related generic-metrics topics to settings/validation (#2935) by @onewland
- feat(mep): fix header filter error (#2938) by @onewland
- build(deps): bump jsonschema from 4.6.0 to 4.7.1 (#2926) by @dependabot
- ref: Remove temporary subscriptions rollout code (#2934) by @lynnagara
- ref: Fix TypedDict import (#2933) by @lynnagara
- feat(mep): add commit log config to generic metrics storages (#2931) by @onewland
- build(deps): bump @types/react-dom from 18.0.5 to 18.0.6 in /snuba/admin (#2924) by @dependabot
- build(deps): bump urllib3 from 1.26.9 to 1.26.10 (#2922) by @dependabot
- feat(mep): do kafka-header filtering to avoid JSON overhead for generic-metrics (#2929) by @onewland
- build(deps): bump @types/react from 18.0.14 to 18.0.15 in /snuba/admin (#2923) by @dependabot
- feat(mep): add commit log topics config (#2928) by @onewland
- feat(profiling): Add functions consumer and dataset (#2894) by @Zylphrex
- build(deps): bump redis from 4.3.3 to 4.3.4 (#2905) by @dependabot
- build(deps): bump python-rapidjson from 1.6 to 1.8 (#2920) by @dependabot
- ref(subscriptions): Remove legacy tick consumer (#2838) by @lynnagara
- chore(deps): Bump sentry-sdk to 1.7.0 to enable baggage propagation (#2927) by @sl0thentr0py
- feat(mep): subscript fix for multiple value arrays (#2914) by @onewland
- fix: Update Flask (#2880) by @evanh
- feat(replacements) Reimplement bypass properly (#2917) by @fpacifici
- fix(build): remove atomicwrites from requirements.txt (#2919) by @onewland
- feat(replacements) Script to execute batches of replacements (#2916) by @fpacifici
- fix(profiling): Disable vertical merge algorithm (#2912) by @phacops
- feat(replays): add replays dataset to dataset factory (#2913) by @JoshFerge

_Plus 41 more_

## 22.6.0

### Various fixes & improvements

- ref(attribution): Split RequestSettings into QuerySetings and attribution (#2808) by @volokluev
- feat: Print the ClickHouse host, port and version if invalid version (#2816) by @lynnagara
- enforce retention_days is int type (#2809) by @MeredithAnya
- fix: Update to latest version of Redis (4.3.3) (#2801) by @evanh
- Update clickhouse-driver from 0.2.2 to 0.2.4 (#2805) by @evanh
- fix: Update frontend packages (#2802) by @evanh
- feat(replays): initial replays clickhouse migration (#2681) by @JoshFerge
- feat: Bump arroyo (#2796) by @lynnagara
- feat(metrics) - raw input table and materialized view for generic metrics sets (#2793) by @onewland
- feat: Remove legacy events subscriptions consumer from Freight (#2798) by @lynnagara
- feat: Don't pass next_offset to message (#2792) by @lynnagara
- fix(rate-limit): Don't override SerializableException constructor (#2797) by @nikhars
- feat(subscriptions): Allow scheduling watermark mode to be overridden (#2791) by @lynnagara
- refactor(metrics): Undo Optional in value processors #2794 (#2794) by @rahul-kumar-saini
- Revert "feat(rate-limit): Add rate limit metrics (#2784)" (#2795) by @nikhars
- ref(subscriptions): Executor created within strategy (#2762) by @MeredithAnya
- feat(rate-limit): Add rate limit metrics (#2784) by @nikhars
- build(deps): bump typescript from 4.7.2 to 4.7.3 in /snuba/admin (#2785) by @dependabot
- build(deps): bump @types/react from 18.0.9 to 18.0.10 in /snuba/admin (#2778) by @dependabot
- feat(subscriptions): Remove legacy transactions subscriptions from Freight (#2728) by @lynnagara
- feat(metrics): create generic sets aggregate table + indices (#2782) by @onewland
- refactor(dlq): Rename policy "closure" to "creator" (#2743) by @rahul-kumar-saini
- chore(tiger): Remove comparing results (#2787) by @nikhars
- Revert "fix(tiger): Implement tupleElement function in snuba (#2687)" (#2786) by @volokluev

_Plus 59 more_

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
