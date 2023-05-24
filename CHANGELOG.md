# Changelog

## 23.5.1

### Various fixes & improvements

- ref(process_message): Tag all metrics by consumer_group (#4226) by @untitaker
- feat: Default join timeout for all consumers of 5 seconds (#4222) by @lynnagara
- fix(clickhouse): Add a check for the max supported clickhouse version (#4158) by @davidtsuk
- fix: Fix flaky optimize test (#4217) by @lynnagara
- fix: Fix DLQ producer config (#4216) by @lynnagara
- ref: bump sentry-arroyo to 2.11.2 (#4215) by @getsentry-bot
- feat: Move DLQ configuration into consumer configuration (#4206) by @lynnagara
- feat: Python and Rust consumers share common cluster resolution code (#4205) by @lynnagara
- Fix slack notifications for when allocation policy changes (#4204) by @volokluev
- ref: bump sentry-kafka-schemas to 0.1.9 (#4207) by @getsentry-bot
- feat(admin): add amount of rows in result set to tracing tool (#4167) by @volokluev
- feat(capman): Allow overriding policy defaults in configuration (#4201) by @volokluev
- fix: All consumers run with --no-strict-offset-reset in dev (#4177) by @lynnagara
- fix(CapMan): Validate Tenant IDs (#4176) by @rahul-kumar-saini
- ref: Use RetryingRedisCluster from sentry-redis-tools (#4197) by @untitaker
- ref(consumer): pass sentry_received_timestamp again (#4198) by @MeredithAnya
- feat(dlq): Add the DLQ instruction mechanism (#4199) by @lynnagara
- fix(capman): create auditlog notifications for allocation policy updates (#4193) by @volokluev
- feat(schema-validation): Validate all messages 😱 (#4194) by @lynnagara
- Revert "ref(consumer): Pass sentry_received_timestamp for e2e latency  (#4052)" (0d1b0cfa) by @getsentry-bot
- ref(consumer): Pass sentry_received_timestamp for e2e latency  (#4052) by @ayirr7
- feat(profiling): Create profiling datasets by default in self-hosted (#4195) by @phacops
- feat: Add metric to count validation failures [SNS-2279] (#4182) by @untitaker
- ref: bump sentry-arroyo to 2.11.1 (#4190) by @getsentry-bot

_Plus 10 more_

## 23.5.0

### Various fixes & improvements

- Pin action-github-commit (#4175) by @chadwhitacre
- ref(tech-debt) Column validator also checks mapped columns (#4116) by @evanh
- Make kafka consumer max poll time configurable (#4165) by @nikhars
- ref: bump sentry-redis-tools to 0.1.5 (#4170) by @getsentry-bot
- ref: bump sentry-kafka-schemas to 0.1.7 (#4168) by @getsentry-bot
- fix: Fix another reference to unavailable secret (#4161) by @untitaker
- Revert "drop old tables (#3896)" (ee62ee06) by @getsentry-bot
- drop old tables (#3896) by @barkbarkimashark
- fix(consumer): Remove DLQ policy for generic metrics consumers config (#4166) by @ayirr7
- fix(consumers): Add timeout for http batch writes (#4160) by @nikhars
- chore(arroyo): Bump version to 2.10.4 (#4162) by @nikhars
- feat(CapMan): AllocationPolicy Config API (#4025) by @rahul-kumar-saini
- feat(functions): Process new functions message (#4056) by @Zylphrex
- fix: Fix permissions of bump-version.yml workflow (#4157) by @untitaker
- feat(CapMan): Basic Snuba Admin UI (#4055) by @rahul-kumar-saini
- ref: Arroyo 2.10.3 (#4153) by @untitaker
- add test referrer to single thread referrers (#4154) by @volokluev
- fix: Unknown functions shouldn't count against the SLO (#4150) by @evanh
- feat(ci): allow skip check migrations (#4111) by @dbanda
- Revert "turn on policy, add user report referrer (#4151)" (21864c36) by @getsentry-bot
- turn on policy, add user report referrer (#4151) by @volokluev
- ref(admin): Give more users access to the admin tool (#4149) by @evanh
- build: sentry-kafka-schemas 0.1.6 (#4147) by @lynnagara
- test: Fix flaky optimize test (#4143) by @lynnagara

_Plus 89 more_

## 23.4.0

### Various fixes & improvements

- fix(CI): add migrations check to main pipeline (attempt 2) (#4019) by @dbanda
- fix: Add killswitch to disable raising of InvalidMessage (#4023) by @untitaker
- add a manually triggered migrations stage to prod gocd (#4014) by @dbanda
- feat(search-issues): process transaction_duration values (#4008) by @barkbarkimashark
- Revert "feat(CI): add migrations check to main pipeline (#4013)" (b59bb7e9) by @getsentry-bot
- build: Arroyo 2.10.1 (#4017) by @untitaker
- feat(CI): add migrations check to main pipeline (#4013) by @dbanda
- feat(CapMan): Errors Allocation Policy V0 (fixed) (#4016) by @volokluev
- ref: Make all errors replacements class-based (#4009) by @untitaker
- Revert "feat(CapMan): AllocationPolicy for Errors (#3999)" (#4015) by @volokluev
- add optional transaction_duration column (#4007) by @barkbarkimashark
- feat(CapMan): AllocationPolicy for Errors (#3999) by @volokluev
- build: sentry-kafka-schemas 0.0.28 (#4005) by @lynnagara
- feat: DLQ for querylog (#4006) by @lynnagara
- fix(search-issues): make resource_id, subtitle, culprit, and level fields available in entity (#3995) by @barkbarkimashark
- build(deps): bump h2 from 0.3.16 to 0.3.17 in /rust_snuba (#4010) by @dependabot
- fix(admin) Fix regex to allow SelectExecutor tracing lines (#3983) by @evanh
- feat: Use new DLQ from Arroyo (#4001) by @lynnagara
- ref: Make delete_groups replacement class-based (#3996) by @untitaker
- build: Arroyo 2.10.0 (#4003) by @lynnagara
- fix: update validation from org_countries for granularity (#4002) by @andriisoldatenko
- fix: Fix type issue (#4000) by @lynnagara
- ref(ci): add auto labeling for migrations (#3961) by @MeredithAnya
- build(deps): bump sentry-arroyo[json] from 2.8.0 to 2.9.1 (#3988) by @dependabot

_Plus 90 more_

## 23.3.1

### Various fixes & improvements

- add file for self hosted settings (#3889) by @enochtangg
- ci: Run Rust linter (#3884) by @lynnagara
- Add sns gcloud project GoCD pipeline (#3850) by @dbanda
- rust: add simple transform and produce strategy example (#3796) by @dbanda
- ref: Rename positions to offsets everywhere (#3888) by @lynnagara
- feat(rust-arroyo): Split the message interface to support batching (#3885) by @lynnagara
- feat(tx_processor): write replay_id as top level column (#3854) by @JoshFerge
- feat(rust-arroyo): Get rid of `Position` (#3883) by @lynnagara
- Remove filtering only local nodes for tracing tool (#3876) by @enochtangg
- fix: Add sentry-compatible alias to install-python-dependencies (#3875) by @untitaker
- fix(replays): set transaction as empty string instead of null (#3878) by @JoshFerge
- feat(schemas): Partially type the data in querylog producer (#3858) by @lynnagara
- feat: The Rust consumer calls Python message processors (#3871) by @untitaker
- feat(rust-consumer): Avoid config files overwriting each other (#3873) by @lynnagara
- ref: Bump sentry-kafka-schemas 0.0.9 (#3882) by @marandaneto
- ref(admin): add CreateTableQuery predefined queries (#3844) by @MeredithAnya
- feat(rust-consumer): Add python processor info to consumer config (#3874) by @lynnagara
- feat(rust-consumer): Remove option to build raw config (#3870) by @lynnagara
- feat: Start building the config for Rust consumer (#3869) by @lynnagara
- Revert "use redis username from env (#3862)" (a03fb448) by @getsentry-bot
- use redis username from env (#3862) by @HydrofinLoewenherz
- feat(rust-consumer): Multistorage consumer (#3865) by @lynnagara
- fix(rust-consumer): Remove auto-initialize feature (#3868) by @lynnagara
- feat(Querylog): Organization ID from tenant_ids again (#3867) by @rahul-kumar-saini

_Plus 2 more_

## 23.3.0

### Various fixes & improvements

- Revert "feat(Querylog): Organization ID from `tenant_ids` (#3857)" (a555678e) by @getsentry-bot
- feat(Querylog): Organization ID from `tenant_ids` (#3857) by @rahul-kumar-saini
- feat(rust-consumer): Actually parse Python settings in Rust (#3860) by @lynnagara
- feat(rust-consumer): Parse Python settings in Rust (#3859) by @lynnagara
- ref(admin): Log when a connection error happens (#3834) by @evanh
- feat: Share Python settings with Rust and add Rust consumer entrypoint (#3856) by @lynnagara
- use correct connection for multi-node clusters (#3846) by @dbanda
- meta: Update post merge hook to warn about updated deps (#3851) by @lynnagara
- ref(admin): dont use tracing user for querylog and remove readonly=2 (#3849) by @MeredithAnya
- ref(schema) Update to 0.0.6 (#3843) by @evanh
- fix(querylog): enable readonly 2 for querylog (#3847) by @MeredithAnya
- fix(test): Add extra query editor test (#3841) by @john-z-yang
- fix(migrations): add replay_id  and exception_main_thread to errors dist ro (#3840) by @MeredithAnya
- test: Run all consumers' message processors against examples (#3842) by @untitaker
- ref: Add typing to all messages in errors replacer (#3835) by @untitaker
- feat(replays): Default event_hash when segment_id is null (#3759) by @cmanallen
- remove dead code from web/query (#3832) by @volokluev
- ref: Bump sentry-kafka-schemas to 0.0.5 (#3839) by @untitaker
- ref(admin) enable events/transactions migration groups (#3838) by @MeredithAnya
- ref: Update mypy to 1.1.1 (#3836) by @untitaker
- feat(querylog): Support for query interpolation in Querylog query editor (#3803) by @john-z-yang
- feat: Add new statuses for SLO (#3747) by @evanh
- ref: Start using python types from sentry-kafka-schemas (#3812) by @untitaker
- fix(CapMan): Ensure tenant_ids metric is accurate (#3831) by @rahul-kumar-saini

_Plus 59 more_

## 23.2.0

### Various fixes & improvements

- fix: Reject improperly typed tag conditions as invalid queries (#3727) by @evanh
- ref(batch-writer): Remove unused terminate() method (#3744) by @lynnagara
- allow strings in select statements on system queries (#3745) by @volokluev
- ref: InsertBatchWriter and ReplacementBatchWriter don't subclass ProcessingStrategy (#3743) by @lynnagara
- add support for enum column type (#3729) by @enochtangg
- change search_issues migrations to non-blocking (#3739) by @barkbarkimashark
- fix: Rename consumer group tag (#3740) by @lynnagara
- fix ui bug where we dont properly refresh table after running (#3733) by @dbanda
- change log error to warn (#3734) by @dbanda
- ref: bump timeout slightly (#3738) by @asottile-sentry
- ref: force ipv4 localhost (#3736) by @asottile-sentry
- add generic metric consumers to freight (#3737) by @enochtangg
- fix(admin): allow querying dist nodes too (#3731) by @dbanda
- Revert "feat(admin): allow querylog to be queried by more threads (#3718)" (#3732) by @volokluev
- migrations: add custom search issues role (#3724) by @dbanda
- docs(clickhouse): describe basic Sentry CH usage (#3701) by @onewland
- ref(migrations): Make ADMIN_ALLOWED_MIGRATION_GROUPS a set (#3720) by @MeredithAnya
- feat(admin): allow querylog to be queried by more threads (#3718) by @volokluev
- feat(replays): add replay_id to transactions table (#3721) by @JoshFerge
- feat(replays): add replay_id to errors table (#3722) by @JoshFerge
- Remove experimental flag from replays (#3726) by @cmanallen
- fix: Fix typing of multistorage consumer (#3719) by @lynnagara
- ref(consumer): Build strategy factory function in multistorage consumer (#3714) by @ayirr7
- fix(cli): Allow more options for datasets and entities (#3715) by @nikhars

_Plus 64 more_

## 23.1.1

### Various fixes & improvements

- ref(DC): Remove Generic Metrics Python (#3650) by @rahul-kumar-saini
- ref(config): Create Replays Entity yaml file  (#3643) by @enochtangg
- ref(config) Create Profiles Entity yaml file (#3642) by @enochtangg
- feat(DC): create events entity, expand testing, fix transactions yaml (#3645) by @volokluev
- refDC): Extract session translation mappers (#3636) by @volokluev
- ref: remove yarn apt list after installation (#3648) by @asottile-sentry
- feat(api-abuse): Add triggered rate limiter name to Querylog stats (#3625) by @enochtangg
- migrations: make querylog match SaaS (#3611) by @dbanda
- build: Arroyo 2.5.0 (#3635) by @lynnagara
- feat: Clean up unused topic (#3634) by @lynnagara
- ref(DC): Add FixedString field to configuration (#3630) by @volokluev
- feat(config): Migrate GroupedMessage and GroupAssignee Entities to YAML (#3616) by @evanh
- ref(migrations): Simplfy policies and status checks (#3606) by @MeredithAnya
- fix(outcomes): Add TTL to outcomes dataset (#3615) by @nikhars
- ref(copy-tables): fix regex (#3631) by @MeredithAnya
- feat(profiling): Add profile id to transactions table (#3607) by @Zylphrex
- ref(config) Migrate Outcomes and OutcomesRaw entities to YAML (#3632) by @evanh
- feat: Tag invalid messages for DLQ with `invalid_message`: `true` (#3633) by @lynnagara
- ref: Remove invalid TODO (#3624) by @lynnagara
- feat: Remove ability to override scheduling mode (#3623) by @lynnagara
- feat(api-abuse): Add timing metrics for table_concurrent (#3628) by @enochtangg
- ref: Remove debugging code (#3621) by @lynnagara
- add settings for codecov (#3627) by @volokluev
- migrations: make errors schema match SaaS (#3604) by @dbanda

_Plus 18 more_

## 23.1.0

### Various fixes & improvements

- feat(DC): Add simple Dataset configs (#3586) by @rahul-kumar-saini
- bug(DC): Add config built storage keys to all_storages dictionary in factory (#3502) by @enochtangg
- bump search_issues storage occurrence_type_id int size (#3593) by @enochtangg
- feat: Change settings.TOPIC_PARTITION_COUNTS to use logical topic name (#3589) by @lynnagara
- feat: Arroyo 2.4.0 (#3585) by @lynnagara
- feat(admin): implement basic admin user roles (#3522) by @MeredithAnya
- fix: Change DefaultNoneColumnMapper to use a normal set (#3580) by @evanh
- update migrations doc to reflect new style (#3584) by @dbanda
- fix(DC): Bring EntityContainsColumnsValidator back to config entities (#3579) by @volokluev
- feat(slices): Make Subscription Scheduler filter by slice ID (#3338) by @ayirr7
- Revert "fix(DC): Bring EntityContainsColumnsValidator back to config entities (#3540)" (#3578) by @volokluev
- Pick up self-hosted CI bugfix (#3577) by @chadwhitacre
- fix(DC): Bring EntityContainsColumnsValidator back to config entities (#3540) by @volokluev
- feat(oncall): add the request_id to the query breadcrumbs (#3561) by @volokluev
- feat(DC): Add Storage selector support for Entity configuration (#3545) by @enochtangg
- feat(issue-platform): increase occurrence_type_id column size from UInt8 to UInt16 (#3575) by @barkbarkimashark
- feat(issue-platform): process extra fields from event (#3571) by @barkbarkimashark
- docs(schema): why do we promote tags? (#3574) by @onewland
- Remove sorting experiment on sql conditions (#3567) by @enochtangg
- ref: Add docstring about ``topic_name`` vs ``get_physical_topic_name``. (#3568) by @lynnagara
- feat(ops): allow disabled entities for storage sets that are not in use (#3572) by @onewland
- test(consumer): Add preliminary unit tests to ConsumerBuilder (#3389) by @ayirr7
- docs(slicing): Add mega-cluster and basic consumer setup (#3433) by @ayirr7
- feat(consumer): Remove parallel collect option (#3555) by @lynnagara

_Plus 39 more_

## 22.12.0

### Various fixes & improvements

- give coverage xml files different paths to prevent overwrites (#3499) by @dbanda
- ref: Remove redundant filter (#3520) by @lynnagara
- ref(settings): Delete unused settings (#3518) by @lynnagara
- ref: Remove the mock batch writer (#3515) by @lynnagara
- feat(DC): ReplacerProcessor Registered + JSON Schema'd + Built (#3505) by @rahul-kumar-saini
- ref: Rewrite Snuba replacer to not use batch processing strategy (#3512) by @lynnagara
- ref(replacements): Move ProjectsQueryFlags out of errors_replacer (#3504) by @rahul-kumar-saini
- Revert "ref: Rewrite the KafkaConsumerWithCommitLog as a strategy (#3506)" (#3510) by @lynnagara
- ref: Rewrite the KafkaConsumerWithCommitLog as a strategy (#3506) by @lynnagara
- capture migration output on stdout (#3508) by @dbanda
- fix(admin): bind log to configure it before using (#3503) by @dbanda
- ref(replacements): Refactor Schema out of ReplacerProcessor init (#3496) by @rahul-kumar-saini
- ref(admin): minor migrations audit log fixes (#3492) by @MeredithAnya
- Add test migration (#3488) by @dbanda
- Rename optimize.py to test_optimize.py (#3498) by @dbanda
- ref(admin): split started/completed audit migrations (#3493) by @MeredithAnya
- ref(migrations): add copy-tables script (#3241) by @MeredithAnya
- add admin UI build to docker image (#3476) by @dbanda
- feat: Arroyo 2.2.0 (#3494) by @lynnagara
- feat(DC): Discover Storage as a Config (#3480) by @rahul-kumar-saini
- feat(CDC): Dynamic Row Processors (#3483) by @rahul-kumar-saini
- Increase optimize redis TTL  (#3484) by @dbanda
- move discover mappers with all the other mappers (#3491) by @volokluev
- ref(abuse) Add more predefined querylog queries (#3489) by @evanh

_Plus 72 more_

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
