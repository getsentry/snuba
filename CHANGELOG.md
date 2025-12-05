# Changelog

## 25.11.1

### Build / dependencies / internal 🔧

- ref: bump sentry-protos to 0.4.7 by @getsentry-bot in [#7559](https://github.com/getsentry/snuba/pull/7559)
- chore: enable trace pagination support by default by @kylemumma in [#7558](https://github.com/getsentry/snuba/pull/7558)
- ref: bump sentry-protos to 0.4.6 by @getsentry-bot in [#7553](https://github.com/getsentry/snuba/pull/7553)
- ref(consumer): change BytesInsertBatch to support builder pattern by @onewland in [#7479](https://github.com/getsentry/snuba/pull/7479)

### New Features ✨

- feat(eap): record item type counts in batches by @onewland in [#7481](https://github.com/getsentry/snuba/pull/7481)
- feat(processor): Handle casted columns in UUID column processor by @Zylphrex in [#7552](https://github.com/getsentry/snuba/pull/7552)

### Bug Fixes 🐛

- fix: subscription executors should not crash when there are too many concurrent queries by @xurui-c in [#7547](https://github.com/getsentry/snuba/pull/7547)
- fix(gocd): fix gocd filters for rs/py deploys by @onewland in [#7545](https://github.com/getsentry/snuba/pull/7545)

### Other

- ref(runtime-config): update write_node_replacements_global default by @MeredithAnya in [#7556](https://github.com/getsentry/snuba/pull/7556)
- feat(query-pipeline): Add configs to customize maximum size of a query by @phacops in [#7546](https://github.com/getsentry/snuba/pull/7546)
- ref(runtime-config): remove *_matview_version by @MeredithAnya in [#7543](https://github.com/getsentry/snuba/pull/7543)
- ref(runtime-config): remove use_split by @MeredithAnya in [#7540](https://github.com/getsentry/snuba/pull/7540)
- meta: Bump new development version in [2fdaba59](https://github.com/getsentry/snuba/commit/2fdaba59535e35903816147913647ed3d4fe60dd)
- ref(runtime-config): remove use_readthrough_query_cache by @MeredithAnya in [#7541](https://github.com/getsentry/snuba/pull/7541)

## 25.11.0

### Various fixes & improvements

- chore(rpc): Add example request for EndpointTraceItemDetails (#7538) by @Zylphrex
- feat(deletes): support TraceItemFilter[s] in DeleteTraceItems API (#7535) by @onewland
- feat(deletes): add support for delete by attribute in config (#7534) by @onewland
- fix(deletes): some ch versions dont have setting (#7536) by @MeredithAnya
- feat: consolidate statsd config to a single environment variable of `SNUBA_STATSD_ADDR` (#7524) by @aldy505
- chore: stop capturing redis oom set failure in sentry (#7528) by @kylemumma
- fix(deletes): properly update clickhouse_settings (#7533) by @MeredithAnya
- gocd: update path filters to match gocd-yaml-config-plugin docs (#7532) by @onewland
- make test less flaky (#7531) by @volokluev
- ref: bump sentry-arroyo to 2.33.0 (#7530) by @getsentry-bot
- chore: add metric to see how many GetTrace request have a user provided limit (#7521) by @kylemumma
- gocd: split deploy triggers take 2 (#7527) by @onewland
- gocd: add filter for deploys for snuba-{py,rs} (#7526) by @onewland
- feat(eap): Insert arrays in EAP (#7514) by @phacops
- gocd: split pipelines, leaving original, update migration check script (#7525) by @onewland
- Revert "[gocd] split snuba deploys into snuba-deploy-py and snuba-deploy-rs (take 2) (#7510)" (4468c1a7) by @getsentry-bot
- Revert "fix(gocd): use deploy-snuba-py-s4s and deploy-snuba-rs-s4s to check m… (#7523)" (505a7270) by @getsentry-bot
- fix(gocd): use deploy-snuba-py-s4s and deploy-snuba-rs-s4s to check m… (#7523) by @onewland
- chore(deps): bump actions/checkout from 4 to 5 (#7517) by @dependabot
- [gocd] split snuba deploys into snuba-deploy-py and snuba-deploy-rs (take 2) (#7510) by @onewland
- ref(lw-deletes): add runtime config lightweight_deletes_sync (#7522) by @MeredithAnya
- fix(trace-item-stats): Apply eap item type filter (#7520) by @shruthilayaj
- feat(eap): Insert client and server sample rates in EAP (#7515) by @phacops
- feat: pagination support in GetTrace endpoint (#7508) by @kylemumma

_Plus 64 more_

## 25.10.0

### Various fixes & improvements

- fix(ignore_case): Handle ignoring casing for LIKE/NOT LIKE (#7452) by @wmak
- fix(eap): Set a better default for timestamp field and customize for logs (#7441) by @phacops
- chore(rust-consumer): add default dlq buffer length (#7429) by @davidtsuk
- fix(cross-item-queries): Allow single filters (#7450) by @wmak
- fix(traceitemstats): only downgrade traceitemstats for AI clients (#7449) by @volokluev
- Modifying Replay Deserializer To Parse Tap Message (#7439) by @cliffordxing
- feat(flextime_routing): Use order by in flextime pagination (#7445) by @volokluev
- fix(ast): fix query formatter not handling dots in column names (#7440) by @volokluev
- feat(replay): Adding Tap Columns (#7437) by @cliffordxing
- ref: bump sentry-arroyo to 2.29.6 (#7444) by @getsentry-bot
- fix tilde in pyproject (#7443) by @bmckerry
- feat(health-check): add limit to individual health check cluster wait (#7436) by @onewland
- feat: TraceItemStats heatmap, basic functionality (#7433) by @kylemumma
- ref(outcomes-routing): always downsample data older than 30 days (#7424) by @MeredithAnya
- for health check errors, enrich events with cluster_name (#7434) by @onewland
- ref(gocd): add healthcheck to de (#7435) by @MeredithAnya
- dep: add granian (#7431) by @gi0baro
- feat(deletes): connect delete RPC endpoint to actual deletion (#7426) by @onewland
- ref: bump sentry-arroyo to 2.29.5 (#7430) by @getsentry-bot
- fix(TraceItemTable): Account for flexible time windows with no results (#7427) by @volokluev
- chore(ci): Remove snuba cloudbuild (#7417) by @hubertdeng123
- ref(devenv): allow more mem for CH (#7425) by @MeredithAnya
- feat(eap): add deletion_settings to eap_items storage definition (#7420) by @onewland
- feat(TracaITemTable): add end_pagination token (#7423) by @volokluev

_Plus 8 more_

## 25.9.0

### Various fixes & improvements

- ci: bump action-build-and-push-images to latest commit hash (#7412) by @Dav1dde
- ref(outcomes): storage selecter, settings, daily storage (try 2) (#7398) by @MeredithAnya
- feat(cbrs): Snuba Admin UI (webpage) (#7399) by @xurui-c
- fix(ci): typo on craft's default release branch name (#7408) by @aldy505
- ref(pyproject.toml): Un-break bump-version.yml workflow (#7403) by @untitaker
- chore(api): Make socket timeout less aggressive in snuba api (#7405) by @volokluev
- Reduce clickhouse send_receive_timeout from 300 to 35 seconds (#7404) by @volokluev
- fix(inc-1340): Make snuba more resilient to redis failures (#7402) by @volokluev
- fix(cbrs): divide subclasses according to their namespaces (#7401) by @xurui-c
- feat(EAP): cross item queries for trace item table (#7385) by @davidtsuk
- fix(issues): resolve u32/u64 group_first_seen bug (#7397) by @thetruecpaul
- feat(cbrs): Snuba Admin endpoints (#7346) by @xurui-c
- fix(observability): raise a specific error for query timeouts (#7364) by @volokluev
- Redo: ref(outcomes): fix partitioning for daily table (#7396) by @xurui-c
- Redo: fix(issues): Process new group_first_seen field on ingest (#7395) by @xurui-c
- Redo: fix: scripts/rust-envvars needs .venv/bin/python (#7394) by @xurui-c
- Stop deploying gauges infra (#7382) by @volokluev
- ref(cbrs): introduce RegisteredClass into ConfigurableComponent (#7379) by @xurui-c
- fix: rust envvars need to be set before devenv/uv sync (#7381) by @joshuarli
- fix(ci): bump timeout for venv recreation (#7380) by @joshuarli
- feat: uv (#7368) by @joshuarli
- fix(search_issues): stop truncating timestamp_ms in search_issues (#7373) by @cvxluo
- ref(cbrs): integrate resource identifier and policy type into AllocationPolicy (#7375) by @xurui-c
- feat(ci): Part 2 Cutover to GHA for image build/push (#7371) by @hubertdeng123

_Plus 10 more_

## 25.8.0

### Various fixes & improvements

- ref: consolidate all dev requirements (#7352) by @joshuarli
- fix: fix broken ci (#7354) by @kylemumma
- fix(gocd): Do not ignore errors from EAP consumers (#7353) by @phacops
- fix(issues): Start processing new_group_first_seen in merge case (#7351) by @thetruecpaul
- chore: Test ClickHouse 25.3 (#7339) by @phacops
- fix(storage-routing): If item type is unspecified, default to spans (#7350) by @volokluev
- ref(eap): Remove more references to spans, eap-spans and eap-items-span (#7301) by @phacops
- fix(storage_routing): fix routing strategy assuming highest accuracy by default (#7347) by @volokluev
- chore: bump sentry-sdk to 2.33.2 again (#7344) by @MeredithAnya
- feat(eap): enable sampling for GetTraces single item queries (#7345) by @volokluev
- ref: bump sentry-arroyo to 2.29.1 (#7343) by @getsentry-bot
- feat: Add Rust support to bump-version workflow (#7342) by @davidtsuk
- feat(errors): Add group_first_seen column to issues platform entity (#7335) by @thetruecpaul
- feat(errors): Add group_first_seen column to issues platform storage (#7336) by @thetruecpaul
- feat(errors): Add group_first_seen column on issues platform (#7334) by @thetruecpaul
- feat(cbrs): unify allocation policy and routing strategy (#7337) by @xurui-c
- feat(eap): add BinaryFormula to AggregationComparisonFilter (#7314) by @onewland
- Revert "chore: Test ClickHouse 25.3 (#7338)" (7e32590f) by @getsentry-bot
- chore: Test ClickHouse 25.3 (#7338) by @phacops
- Revert "chore: Test ClickHouse 25.3 (#7332)" (4854e6cb) by @getsentry-bot
- chore: Test ClickHouse 25.3 (#7332) by @phacops
- ref(subscriptions): crash schedulers that dont have LogAppendTime on default topic (#7325) by @MeredithAnya
- feat(clickhouse): Add missing user config for ClickHouse in dev (#6868) by @hubertdeng123
- cleanup(consumers): remove slice and custom envoy request timeout (#7328) by @volokluev

_Plus 45 more_

## 25.7.0

### Various fixes & improvements

- obs(consumers): Emit a metric when a write to clickouse fails (#7276) by @volokluev
- fix(manual_jobs): use a redis pipeline instead of mget for manual jobs (#7230) by @volokluev
- feat(eap): Allow to coalesce specific attributes (#7272) by @phacops
- ref(deps): Bump arroyo to 2.27.0 (#7270) by @untitaker
- feat(attributes): add sampling to autocomplete (#7267) by @volokluev
- Revert "feat(eap): Allow to coalesce specific attributes (#7269)" (beabc915) by @getsentry-bot
- feat(eap): Allow to coalesce specific attributes (#7269) by @phacops
- Revert "Use v2 consumer everywhere (#7262)" (a5740c8b) by @getsentry-bot
- Use v2 consumer everywhere (#7262) by @volokluev
- chore(consumers): Bump Rust toolchain version (#7252) by @lcian
- meta: Bump new development version (5451c41d)

## 25.6.2

### Various fixes & improvements

- chore(consumers): bump statsdproxy (#7256) by @lcian
- feat: enable reliability of formulas to be returned in trace_item_table endpoint (#7226) by @kylemumma
- fix(TraceItemsDetails): dedupe int,bool and float attrs (#7260) by @MeredithAnya
- remove sentry logging (#7263) by @MeredithAnya
- chore(traces): Remove standalone spans condition (#7257) by @Zylphrex
- fix(ci): Tag snuba docker images using PR commit shas (#7259) by @hubertdeng123
- feat(logs): Add support for creating logs subscriptions (#7261) by @shruthilayaj
- feat: open fail for snuba cache write failures (#7255) by @kylemumma
- feat(consumers): Re-implement batching consumer (#7248) by @volokluev
- Revert "chore(consumers): Bump statsdproxy (#7251)" (889d1e26) by @getsentry-bot
- chore(consumers): Bump statsdproxy (#7251) by @lcian
- ci: acquire docker image from ghcr for release (#7245) by @aldy505
- ref: bump sentry-protos to 0.3.0 (#7249) by @getsentry-bot
- ref: bump sentry-protos to 0.2.1 (#7247) by @getsentry-bot
- fix(cbrs): set request in routing decision (#7241) by @xurui-c
- chore(eap-timeseries): Bump max allowed bucktes by 1 (DAIN-667) (#7246) by @shruthilayaj
- chore(devservices): Bump devservices to 1.2.1 (#7244) by @hubertdeng123
- meta: Bump new development version (2aee9c59)

## 25.6.1

### Various fixes & improvements

- feat(capman): Fix model ID for workflow querying referrer (#7243) by @kcons
- Revert "feat(capman): Fix model ID for workflow querying referrer (#7239)" (b8889e71) by @getsentry-bot
- feat(capman): Fix model ID for workflow querying referrer (#7239) by @kcons
- meta: Bump new development version (57f19402)

## 25.6.0

### Various fixes & improvements

- ref(eap): tests with orderby label only (#7236) by @MeredithAnya
- feat(devservices): Add containerized modes for profiles, uptime, metrics dev (#7229) by @hubertdeng123
- feat(cbrs): move routing strategies to rpc (#7201) by @xurui-c
- feat(capman): Don't rate limit customers for workflow alert referrers (#7237) by @kcons
- ci: provide arm64 image build (#6825) by @aldy505
- chore(redis): update sentry-redis-tools to 0.5.0 (#7232) by @davidtsuk
- fix(migration): only drop the first bucket to speed up the migration (#7228) by @davidtsuk
- feat(consumers): Create V2 consumer copy (#7227) by @volokluev
- chore(eap): Remove hash map columns for buckets 0 to 4 (#7225) by @phacops
- chore(eap): Remove hashed_keys column (#7224) by @phacops
- fix(eap): Remove AddIndex on a column that doesn't exist (#7223) by @phacops
- fix(eap): Remove hash map columns from the MV (#7222) by @phacops
- fix(eap): Reduce impact of migration (#7221) by @phacops
- Revert "chore(eap): drop hashed key columns" (#7220) by @onewland
- Revert "Deprecate TOPIC_PARTITION_COUNTS setting (reverts ed0426b) (#5942)" (e8188c4b) by @getsentry-bot
- Deprecate TOPIC_PARTITION_COUNTS setting (reverts ed0426b) (#5942) by @lynnagara
- fix(eap): fix stupid attribute values query for item id (#7219) by @volokluev
- chore(eap): drop hashed key columns (#7218) by @davidtsuk
- chore(eap): Remove eap-spans resolver (#7215) by @phacops
- chore(eap): remove _hash_map columns from eap config (#7217) by @volokluev
- fix(EAP-100): allow passing in string timestamps in filters (#7202) by @volokluev
- feat(consumers): set default join timeout on rust consumers (#7213) by @volokluev
- fix(eap): don't create a column if it already exists (#7211) by @davidtsuk
- feat(eap): add hashed attributes column (#7208) by @davidtsuk

_Plus 38 more_

## 25.5.1

### Various fixes & improvements

- chore(ci): Bump sentry-related dependencies more aggressively (#6769) by @untitaker
- ref(rust-snuba): Add architecture doc (#7151) by @untitaker
- feat(trace-items): Support page tokens in trace item attributes endpo… (#7182) by @Zylphrex
- Revert "fix(eap): Translate eap_spans to eap_items_span for subscriptions (#7181)" (ce427610) by @getsentry-bot
- chore(eap-alerts): Clean up devserver args to reflect correct entity (#7185) by @shruthilayaj
- feat(replay): update processor for user_geo fields (#7175) by @aliu39
- chore: Create subscriptiosn eith eap items spans entity (#7183) by @shruthilayaj
- feat(replay): update entity/storage yamls for user_geo fields (#7174) by @aliu39
- fix(eap): Translate eap_spans to eap_items_span for subscriptions (#7181) by @phacops
- meta: Bump new development version (baa701cd)

## 25.5.0

### Various fixes & improvements

- feat(replay): add migration for user geo fields (#7173) by @aliu39
- chore(eap): Cleanup EAP span, log and mutation references (#7178) by @phacops
- feat(api): Support multipart/form-data to receive binary data (#7180) by @phacops
- fix(job): log correct query for truncating EAP spans (#7179) by @onewland
- feat(replay): Add OTA Updates Context to the processor (#7166) by @krystofwoldrich
- feat(replay): Add Dataset & Storage YAML for OTA Updates Context (#7165) by @krystofwoldrich
- feat(replay): Add Migration for OTA Updates Context Columns (#7164) by @krystofwoldrich
- feat(eap): Transform an order by on timestamp to the full sort key order to improve performance  (#7153) by @xurui-c
- create manual job for truncating eap_spans_2_local tables (#7167) by @onewland
- fix(rust): Validate schemas according to their type (#7176) by @phacops
- fix: fix duplicate labels in RPC causing incorrect behavior (#7139) by @kylemumma
- fix batch join timeout reading from env var (#7171) by @volokluev
- fix(trace-items): Should return keys in filter (#7162) by @Zylphrex
- chore(devservices): Bump version to 1.1.5 and add programs.conf file (#7161) by @hubertdeng123
- chore(eap): Stop trimming query time ranges (#7159) by @phacops
- Revert "Revert "feat(eap): Add an item consumer (#7122)"" (#7160) by @volokluev
- Revert "feat(eap): Add an item consumer (#7122)" (0b2e3280) by @getsentry-bot
- feat(eap): Add an item consumer (#7122) by @phacops
- ref(consumer) only log when there are rows to write (#7155) by @MeredithAnya
- Revert "feat(eap): Transform an order by on timestamp to the full sort key order to improve performance (#7149)" (b4fbd4c6) by @getsentry-bot
- fix(eap): Remove max execution time from attribute names endpoint (#7152) by @volokluev
- chore(eap): Add a eap_items_span entity for backwards compatibility (#7150) by @phacops
- feat(eap): Transform an order by on timestamp to the full sort key order to improve performance (#7149) by @phacops
- fix(ourlogs): Do not store span_id (#7148) by @colin-sentry

_Plus 55 more_

## 25.4.0

### Various fixes & improvements

- fix(eap): Cast timestamp as a float to and then make it a datetime (#7072) by @phacops
- feat(rust-snuba): Enable SSL in rdkafka (#7059) by @untitaker
- ref: bump sentry-kafka-schemas to 1.2.0 (#7069) by @getsentry-bot
- ref: bump sentry-protos to 0.1.69 (#7070) by @getsentry-bot
- feat(sourecmaps) - Allow querying 'symbolicated_in_app' field on events and discover (#7063) by @yuvmen
- Set timeout_overflow_mode to break on all tier 1 queries (#7065) by @volokluev
- obs(snuba): add query settings to db span (#7064) by @volokluev
- perf(sampling-in-storage): track timeout queries (#7061) by @xurui-c
- Set timeout_before_checking_execution speed to 0 (#7062) by @volokluev
- fix(sampling-in-storage): query scans 0 bytes (#7055) by @xurui-c
- feat(group-by-alerts): Allow for group by's without conditions on the events dataset (#7057) by @JoshFerge
- feat(eap): add a sample count threshold for percentile reliability (#7054) by @davidtsuk
- feat(sourcemaps) - Add `symbolicated_in_app` column on errors (#7043) by @yuvmen
- feat: remove logs resolver, endpoint itemstats (#7042) by @kylemumma
- perf(sampling-in-storage): emit raw estimation error (#7051) by @xurui-c
- fix(sourcemaps) - Added `symbolicated_in_app` column to errors_ro (#7052) by @yuvmen
- fix(admin): Do not scrub UUIDs, hex strings and datetimes (#7053) by @untitaker
- feat(sourcemaps) - Add new column `symbolicated_in_app` on errors (#7044) by @yuvmen
- feat: eap timeseries works for logs (#7047) by @kylemumma
- fix(sampling-in-storage): fix increment metric (#7049) by @xurui-c
- fix(eap-items): add the sampling factor column to the distributed downsampled tables (#7048) by @davidtsuk
- bump max clickhouse version to 24.8.14.10459 (#7046) by @MeredithAnya
- fix(eap-items): Write to new sampling_factor column (#7020) by @davidtsuk
- fix(eap-items): Update materialized views to include the sampling factor column (#7021) by @davidtsuk

_Plus 60 more_

## 25.3.0

### Various fixes & improvements

- feat(smart-autocomplete): Implement smart autocomplete functionality (#6960) by @volokluev
- fix(eap): remove items_attribute_mv (#6968) by @volokluev
- fix(eap): allow hashmaps to be used in the entity (#6967) by @volokluev
- fix(eap-items): add attribute mappings for backwards compatibility (#6961) by @davidtsuk
- feat: storage for new item_attrs table (#6944) by @kylemumma
- feat: new mv for item_attrs (#6956) by @kylemumma
- fix(admin): fix log profile events nonsense (#6965) by @volokluev
- fix(eap): Return high reliability when sampling rate is 100% (#6954) by @jan-auer
- fix(eap-items): swap description and raw_description (#6958) by @davidtsuk
- fix(eap-items): make sure we are not overriding sentry.description (#6955) by @davidtsuk
- ref(replay): add environment to ReplayClickEvent (#6946) by @michellewzhang
- feat(smart_autocomplete): storage definition for new materialized view (#6951) by @volokluev
- fix(eap): Update relative confidence calculation (#6935) by @jan-auer
- feat: drop item_attrs mv (#6953) by @kylemumma
- chore: increase timeout for some of the ci tests (#6952) by @kylemumma
- Remove unsupported comments deploy.sh (#6950) by @volokluev
- fix(eap-items): map sentry.name to sentry.description in eap_items (#6949) by @davidtsuk
- Remove vector deployment again (#6948) by @volokluev
- Revert "remove test-eap-mutations-vector from deploy script (#6947)" (342d8207) by @getsentry-bot
- remove test-eap-mutations-vector from deploy script (#6947) by @volokluev
- feat: item_attrs materialized views for new eap_items table (#6943) by @kylemumma
- feat(eap-sampling): add mv migration for sampled views (#6940) by @volokluev
- SRE-630: add snuba container selector for migration (#6945) by @mwarkentin
- fix(gocd): Fix missing/changed container names (#6942) by @rgibert

_Plus 52 more_

## 25.2.0

### Various fixes & improvements

- feat(uptime): Add incident_status column to uptime_monitor_checks (#6886) by @evanpurkhiser
- chore(eap): Enable all storages in every region (#6884) by @phacops
- cleanup(gen-metrics): remove meta_tag_values_mv (#6882) by @onewland
- Revert "Revert "remove generic_metrics/*/distributions_meta_tag_value… (#6883) by @onewland
- chore(docs): update references to devservices (#6881) by @shellmayr
- Revert "remove generic_metrics/*/distributions_meta_tag_values (#6878)" (2dea1189) by @getsentry-bot
- remove generic_metrics/*/distributions_meta_tag_values (#6878) by @onewland
- fix(snuba-admin): stop crashing when turning off profile events (#6879) by @volokluev
- chore(eap): Restore missing materialized views (#6877) by @phacops
- feat(eap): Add hash map of attribute keys to the items table (#6876) by @phacops
- migrate(smart-autocomplete): Add a hashmap optimization + BF index to smart autocomplete table (#6875) by @volokluev
- feat(consumer): add flag for custom envoy request timeout (#6874) by @onewland
- fix(eap): Bad circular import in endpoint get traces (#6873) by @Zylphrex
- chore: Upgrade clickhouse-driver to 0.2.9 (#6869) by @phacops
- feat: eap support formulas in timeseries endpoint (#6854) by @kylemumma
- feat(eap): Add a table to store items (#6850) by @phacops
- feat(capman): Remove BytesScannedWindowAllocationPolicy from errors_ro (#6866) by @volokluev
- fix(eap): reliabilities should be updated even when value is null (#6863) by @davidtsuk
- Revert "cleanup(smart_autocomplete): remove smart autocomplete mv (#6867)" (0bdf9a9a) by @getsentry-bot
- cleanup(smart_autocomplete): remove smart autocomplete mv (#6867) by @volokluev
- fix: bump sentry kafka schema (#6864) by @JoshFerge
- feat: Add a Bool column type (#6861) by @phacops
- feat: Allow to use 128 bits integers (#6862) by @phacops
- chore: bump sentry-kafka-schemas (#6857) by @JoshFerge

_Plus 76 more_

## 25.1.0

### Various fixes & improvements

- fix: snuba admin system query error messaging (#6763) by @kylemumma
- feat: Add SSL/TLS support for ClickHouse connections (#6459) by @patsevanton
- fix(settings): specify VALID_RETENTION_DAYS for self-hosted (#6756) by @aldy505
- chore(deps): Bump Python to 3.11.11 (#6719) by @beninabox
- Bump arroyo version (#6766) by @volokluev
- feat(ourlogs): Add entity/storage configs & dev worker (#6759) by @colin-sentry
- fix(optimize): make tests and optimize use UTC timestamps (#6760) by @onewland
- meta: Bump new development version (1fe61a9d)

## 24.12.2

### Various fixes & improvements

- ref(uptime): use new v2 table in storage (#6761) by @JoshFerge
- ref(uptime): use region instead of region_slug (#6762) by @JoshFerge
- ref(uptime): rebuild uptime storage table (#6758) by @JoshFerge
- chore(eap): write script to send scrubbed data into a gcs bucket (#6698) by @davidtsuk
- fix(scripts): Grab everything in the path (#6750) by @phacops
- feat(profiling): process environment value for profile_chunks and add the column to the storage definition (#6738) by @viglia
- ci: Switch e2e test to self-hosted repo (#6746) by @BYK
- feat(ourlogs): Add a kafka consumer (#6743) by @colin-sentry
- Revert "deps: replace python-jose with pyjwt (#6739)" (96d5c2bd) by @getsentry-bot
- fix(uptime): add snapshot test for corrected typo (#6744) by @JoshFerge
- feat(sudo): allow dropping of replica in sudo tool (#6742) by @volokluev
- chore(codeowners): add crons team to uptime files (#6741) by @JoshFerge
- deps: replace python-jose with pyjwt (#6739) by @mdtro
- fix(uptime): align rust consumer with clickhouse schema (#6740) by @JoshFerge
- feat(devservices): Support no workers option for containerized version of snuba (#6737) by @hubertdeng123
- feat(devservices): Add devservices ci validation job (#6716) by @hubertdeng123
- chore(uptime): move uptime storage to partial (#6736) by @JoshFerge
- feat(profiling): add environment column to the profile_chunks table (#6722) by @viglia
- feat(EAP): Trace Item resolvers (#6732) by @volokluev
- feat(ourlogs): Simplify the buckets for logs (#6735) by @colin-sentry
- Revert "chore(eap-spans): Take advantage of parallel reads (#6579)" (f8900d8c) by @getsentry-bot
- Implement filter offset for attribute values API (#6667) by @xurui-c
- fix(eap): use TDigestWeighted instead of TDigest for perncetile confidence calculation (#6734) by @shellmayr
- fix(storage): correctly name storage key (#6733) by @JoshFerge

_Plus 28 more_

## 24.12.1

### Various fixes & improvements

- Revert "fix(eap-spans): Add an index on project_id (#6695)" (9f5c3f57) by @getsentry-bot
- fix(eap-spans): Add an index on project_id (#6695) by @phacops
- 984: scrub eap_spans_str_attrs (#6694) by @kylemumma
- fix(inc984): scrub the correct bucket for sentry.user (#6693) by @xurui-c
- fix(inc984): scrub the correct bucket for sentry.user.ip (#6692) by @volokluev
- inc984: scrub `user` from eap_spans (#6691) by @xurui-c
- inc984: scrub `user` from spans (#6689) by @xurui-c
- fix(rust): Honor exit code (#6674) by @untitaker
- ref(admin): lw delete related system queries (#6685) by @MeredithAnya
- chore: Bump rust-toolchain (#6688) by @untitaker
- chore(devservices): Bumping the version of devservices to latest (#6682) by @IanWoodard
- fix(devservices): Add orchestrator devservices label to clickhouse (#6687) by @hubertdeng123
- fix(eap): Fix divide by 0 errors caused when the sample count is 0 (#6681) by @davidtsuk
- meta: Bump new development version (44657332)
- ref(lw-deletes): add project_id killswitch and some logging (#6677) by @MeredithAnya
- docs: update CH supported versions (#6683) by @MeredithAnya
- feat(inc-984): Cleanup EAP spans  (#6676) by @xurui-c
- fix(admin): Allow special characters in SYSTEM/OPTIMIZE queries (#6680) by @evanh
- feat(eap): Use weighted average instead of simple average for calculating average sampling rate (#6678) by @davidtsuk
- ref(lw-deletes): concurrent allocation policy requires org id (#6679) by @MeredithAnya

## 24.12.0

### Various fixes & improvements

- feat(inc-984): store project ids list in dictionary in scrub job (#6675) by @volokluev
- ref(lw-deletes): enforce ratelimiter (#6644) by @MeredithAnya
- fix(admin): Allow KILL MUTATION commands in sudo mode (#6672) by @evanh
- fix(inc984): align start/end timestamp to partition boundaries (#6670) by @volokluev
- chore(deps): bump relay from 0.9.2 to 0.9.4 (#6660) by @jjbayer
- feat(inc984): make mutation condition simpler (#6669) by @volokluev
- chore: Bump Arroyo to 2.19.5 (#6666) by @ayirr7
- ref: bump sentry-arroyo to 2.19.4 (#6663) by @getsentry-bot
- fix(eap-alerts): Fix subscriptions referrer for eap alerts (#6662) by @shruthilayaj
- chore(api): Do not log healthcheck error if downfile exists (#6635) by @untitaker
- feat(eap): add additional validation for group by  (#6659) by @davidtsuk
- feat(eap): add default value to virtual column (#6657) by @davidtsuk
- ref: bump sentry-arroyo to 2.19.3 (#6656) by @getsentry-bot
- Implement filter offset for attribute keys API (#6618) by @xurui-c
- feat: make sentry RPC instrumentation more specific to the endpoint (#6654) by @kylemumma
- fix(consumers): Respect 60 day retention days period (#6631) by @volokluev
- feat: add missing example in admin rpc tool (#6647) by @kylemumma
- hotfix(inc-984): Add manual job to scrub IPs from spans (#6649) by @volokluev
- feat: support 15 minute granularity on eap time series RPC (#6645) by @kylemumma
- fix(eap): Fix divide by 0 bug (#6653) by @davidtsuk
- fix: run sentry tests when RPC changes (#6652) by @colin-sentry
- meta: Bump new development version (60ff5441)
- chore(eap-spans): Take advantage of parallel reads (#6579) by @phacops

## 24.11.2

### Various fixes & improvements

- feat(eap): Bump max timeseries buckets (#6630) by @volokluev
- fix(scripts): Automatically create local tables before MV (#6648) by @untitaker
- feat(eap): Implement confidence intervals for percentiles (#6634) by @davidtsuk
- fix: admin rpc endpoint selector bug fix (#6641) by @kylemumma
- ref(admin): Allow merge function in FROM clause (#6643) by @evanh
- fix: Actual Rust support in bump-version (#6640) by @untitaker
- fix 0006_sorting_key_change migration (#6633) by @MeredithAnya
- chore: Bump Arroyo 2.19.2 (#6639) by @ayirr7
- chore: Bump Arroyo to 2.19.1 (#6632) by @ayirr7
- chore(devservices): Bumping the version of devservices to latest (#6629) by @IanWoodard
- feat(eap-api): support in conditions in TraceItemFilter (#6623) by @kylemumma
- Revert "ref(ci): Disable self-hosted e2e CI due to resource constraints" (#6622) by @hubertdeng123
- feat(devservices): Use https for repo links (#6627) by @hubertdeng123
- chore: Bump rust-arroyo to published crate (#6614) by @untitaker
- feat(label): adding label to container (#6628) by @IanWoodard
- consumer: Add flag to configure DLQ buffer limit (#6626) by @ayirr7
- feat(runtime_config): A way to add static descriptions to config items (#6624) by @untitaker
- fix(admin): add example RPC call for the endpoints missing it (#6615) by @kylemumma
- feat(eap-api): Use signs in count, average, and sum calculations (#6613) by @davidtsuk
- feat(devservices): Add healthchecks to clickhouse and snuba (#6616) by @hubertdeng123
- feat(memory): Add script to debug consumer OOM issues (#6585) by @nikhars
- ref(ci): Disable self-hosted e2e CI due to resource constraints (#6620) by @hubertdeng123
- ref(admin): Allow clusterAllReplicas in system queries (#6619) by @evanh
- ref(profiling): start the profile_chunks consumer when ENABLE_PROFILES_CONSUMER is true (#6607) by @viglia

_Plus 11 more_

## 24.11.1

### Various fixes & improvements

- ref(clickhouse-24.3): allow_suspicious_primary_key for eap migration (#6602) by @MeredithAnya
- chore(devservices): Bumping the version of devservices to latest (#6596) by @IanWoodard
- feat(consumers): Quantized rebalancing in Rust (#6595) by @untitaker
- chore(metrics-summaries): Remove metrics summaries consumer code (#6590) by @phacops
- feat(eap): Extract profile id for eap spans (#6597) by @Zylphrex
- fix(codeowners): Code owners for eap processor (#6598) by @Zylphrex
- fix(eap): fix bug where we pass non str group by mapping to timeseries (#6593) by @davidtsuk
- fix(eap-api): delimeter for alias (#6592) by @xurui-c
- feat(eap): Add confidence interval calculations for count (#6568) by @davidtsuk
- RPC handles same name attributes (#6581) by @xurui-c
- log(optimize): after optimize, log how long it took w/partition (#6591) by @onewland
- feat(devservices): Add restart settings and use external devservices network (#6587) by @hubertdeng123
- chore(deps): bump actions/checkout from 2 to 4 (#6554) by @onkar
- chore(devservices): Bumping the version of devservices to latest (#6582) by @IanWoodard
- chore(ci): Upgrade ClickHouse to v23.8 for CI (#6584) by @phacops
- feat(subscriptions): Add a create subscription rpc endpoint (#6571) by @shruthilayaj
- fix(script): Wait for futures when producing to kafka (#6583) by @nikhars
- fix: convert max_batch_time seconds (#6580) by @MeredithAnya
- fix: dont set group.instance.id incorrectly (#6578) by @MeredithAnya
- chore(eap-spans): Increase the number of replicas to scan in parallel (#6577) by @phacops
- V1 RPC of TraceItemAttributeValues (#6563) by @xurui-c
- meta: Bump new development version (9b923342)
- build(deps): bump types-python-dateutil from 2.8.19 to 2.8.19.14 (#6557) by @onkar
- build(deps-dev): bump webpack-cli from 4.10.0 to 5.1.4 in /snuba/admin (#6558) by @onkar

_Plus 4 more_

## 24.11.0

### Various fixes & improvements

- feat(consumers): rust consumers quantized rebalance (#6561) by @volokluev
- chore(deps): bump docker/setup-buildx-action from 2 to 3 (#6553) by @onkar
- ref(deletes): add lw-deletions-search-issues-consumer to deploy.sh (#6567) by @MeredithAnya
- fix(admin): ignore gather_profile_events for certain storages (#6566) by @MeredithAnya
- chore(deps): bump getsentry/action-github-app-token from 2.0.0 to 3.0.0 (#6549) by @onkar
- ref(deletes): add use_bulk_deletes runtime config (#6560) by @MeredithAnya
- feat(devservices): Only expose ports to localhost (#6565) by @hubertdeng123
- chore(deps): bump types-pyyaml from 6.0.11 to 6.0.12.20240808 (#6552) by @onkar
- chore(deps): bump getsentry/action-migrations from 1.0.8 to 1.2.2 (#6556) by @onkar
- feat(eap): Add support for extrapolation (#6536) by @davidtsuk
- feat(devservices): Bump devservices version (#6507) by @hubertdeng123
- chore(eap-spans): Remove unused tables (#6544) by @phacops
- chore(eap-spans): Switch test to use new spans table (#6550) by @phacops
- [CapMan visibility] rejected queries are ran with 0 threads (#6545) by @xurui-c
- fix: ENABLE_LW_DELETIONS_CONSUMER default to False (#6546) by @MeredithAnya
- ref(deletes): bulk delete consumer (#6510) by @MeredithAnya
- chore(deps): bump packaging from 21.3 to 24.1 (#6540) by @onkar
- chore(deps): bump progressbar2 from 4.0.0 to 4.2.0 (#6534) by @onkar
- chore(deps): bump jsonschema from 4.16.0 to 4.23.0 (#6539) by @onkar
- chore(deps): bump google-github-actions/auth from 1 to 2 (#6541) by @onkar
- build(deps): bump sentry-usage-accountant from 0.0.10 to 0.0.11 (#6542) by @onkar
- build(deps): bump actions/setup-node from 3 to 4 (#6543) by @onkar
- Fix sampling weight calculation (#6537) by @davidtsuk
- chore(deps): bump types-setuptools from 65.3.0 to 74.1.0.20240907 (#6526) by @onkar

_Plus 102 more_

## 24.10.0

### Various fixes & improvements

- ref(functions): refactor functions processor and storage (#6411) by @viglia
- fix(eap): remove flask stuff from rpc (#6410) by @volokluev
- fix(admin): Handle NaN in admin results (#6412) by @evanh
- fix(database-clusters): Use a lock when creating connection (#6407) by @davidtsuk
- feat(rpc): handle exceptions by returning a proto and non-200 (#6402) by @colin-sentry
- ref(eap): Clean up the mutations interface (#6344) by @untitaker
- feat(admin): Show data in clickhouse tracing tool (#6406) by @untitaker
- feat(database-clusters): Use a ThreadPoolExecutor to speed up node fetching (#6405) by @davidtsuk
- feat(profiling): add new columns to the raw_functions table (#6398) by @viglia
- fix(database-clusters): Remove Query Node column (#6404) by @davidtsuk
- feat(eap): Port TraceItemTable endpoint to v1 (#6401) by @volokluev
- feat: Add EAP span subscriptions for devserver (#6399) by @shruthilayaj
- ref(deletes): increase max rows to 100000 (#6400) by @MeredithAnya
- Revert "feat: Add EAP span subscriptions for devserver (#6396)" (ff049323) by @getsentry-bot
- feat: Add EAP span subscriptions for devserver (#6396) by @shruthilayaj
- feat(eap): record rpc endpoint success and timing (#6392) by @volokluev
- feat(job-runner) view logs in custom jobs admin page (#6394) by @onewland
- feat(job-runner): persist logs in Redis for easier viewing in snuba admin (#6387) by @onewland
- fix(devservices): Fixing devservices config (#6393) by @IanWoodard
- fix(database-clusters): Use host name instead of address when fetching system settings (#6391) by @davidtsuk
- fix: better error message in admin system query (#6390) by @kylemumma
- feat(database-clusters): Add server settings for each node and additional data (#6386) by @davidtsuk
- feat(job-runner): run job from snuba admin (#6385) by @onewland
- feat(capman): remove legacy cap on threads (#6389) by @volokluev

_Plus 73 more_

## 24.9.0

### Various fixes & improvements

- Update migrations list command to show migrations that no longer exist in the codebase (#6299) by @davidtsuk
- metric(consumer): Add a metric to track the size of individual spans (#6300) by @ayirr7
- feat(rpc): Update tags list rpc (#6301) by @Zylphrex
- feat(eap): add virtual column support (#6292) by @volokluev
- tweak(eap): Allow more memory usage for eap spans (#6298) by @volokluev
- ref(doc): add documentation for the ReadinessState enum (#6295) by @viglia
- feat(eap): Start ingesting data into sample_weight_2 column (#6290) by @colin-sentry
- Update docker entrypoint to run heaptrack  (#6273) by @ayirr7
- fix(eap): Switch to sampling_weight_2 in entity (#6287) by @colin-sentry
- bug(query): Run entity validators in composite query pipeline (#6285) by @enochtangg
- feat(eap): make mapContains work with EAP dataset (#6284) by @colin-sentry
- feat(job-runner): create a new `snuba jobs` command (#6281) by @xurui-c
- feat(eap): Shard meta tables by trace ID (#6286) by @colin-sentry
- fix(eap): Make span_id be returned as a string correctly (#6283) by @colin-sentry
- feat(job-runner): scaffolding for job manifest testing (#6282) by @onewland
- bug(admin): Fix invalid query error alerting in snuba admin (#6280) by @enochtangg
- Fixing Snuba Admin trace UI error. (#6278) by @nachivrn
- feat(eap): Add a processor that allows you to do mapKeys on attr_str (#6277) by @colin-sentry
- cleanup(capman): remove legacy table rate limits (#6274) by @volokluev
- Fixing Snuba Admin trace UI error. (#6276) by @nachivrn
- hackweek(snuba-admin): MQL query tool (#6235) by @enochtangg
- feat(eap): Endpoint to get the tags available for a project (#6270) by @colin-sentry
- feat(sudo): issue slack notifications when sudo mode is used (#6271) by @volokluev
- chore(eap): Add entities and storages for EAP span meta tables (#6269) by @colin-sentry

_Plus 60 more_

## 24.8.0

### Various fixes & improvements

- switch readiness state to limited to skip migrations and unblock CI (#6210) by @volokluev
- fix: Add DropIndices back as it was removed after a revert (#6209) by @phacops
- fix(eap): fix number migration collision (#6208) by @volokluev
- Revert "fix(eap-spans): Drop ineffective indices to speed up insertion (#6206)" (68826531) by @getsentry-bot
- ref(snuba-deletes) don't run delete queries when 0 rows need to be deleted (#6199) by @xurui-c
- fix(eap-spans): Drop ineffective indices to speed up insertion (#6206) by @phacops
- ref(eap): Remove materialized view (#6205) by @evanh
- fix: deletes api, change success response format, fix admin bug (#6197) by @kylemumma
- feat(eap): stub API for aggregate bucket request (#6204) by @volokluev
- ref(eap): Change sampling_weight to a UInt (#6190) by @evanh
- feat(eap): Aggregate request protobuf endpoint  (#6202) by @volokluev
- feat(eap): Add the TimeSeriesProcessor to entities/eap_spans (#6195) by @colin-sentry
- feat(EAP) Add Make directive that creates proper import paths (#6196) by @colin-sentry
- fix(capman): sets the correct throttle policy in query result metadata when multiple throttle policies present (#6192) by @xurui-c
- feat(generic-metrics): Bump materialization_version for generic metrics (#6194) by @john-z-yang
- feat: make deletes endpoint compatible with snuba SDK (#6193) by @kylemumma
- Change CODEOWNERS for EAP team (#6187) by @colin-sentry
- feat(generic-metrics): Forward sampling information from consumer to clickhouse (#6177) by @john-z-yang
- Increase the default throttle/warning threshold for allocation policies (#6189) by @xurui-c
- fix(eap): Use arrayElement directly to fix alias issues with HashBucketMapper (#6188) by @colin-sentry
- fix: Migrations ended up with the same number (#6183) by @evanh
- ref(snuba-deletes): introduce delete allocation policies (#6180) by @MeredithAnya
- feat(eap): Add an example endpoint that uses protobuf over http (#6173) by @colin-sentry
- feat(generic-metrics): Add migrations to allow sampling for distributions (#6172) by @enochtangg

_Plus 40 more_

## 24.7.1

### Various fixes & improvements

- feat(cleanup): remove complicated readthrough cache (#6130) by @volokluev
- fix(transactions): Select transactions entity when using profiler id (#6133) by @Zylphrex
- Revert "Add processor for new spans schema (#6123)" (d60b3e6f) by @getsentry-bot
- Revert "Define the models for the new Events Analytics Platform (#6126)" (b1376d69) by @getsentry-bot
- Add processor for new spans schema (#6123) by @colin-sentry
- Define the models for the new Events Analytics Platform (#6126) by @colin-sentry
- code (#6129) by @xurui-c
- fix: wait on system.mutations between hosts during migration execution (#6121) by @kylemumma
- feat(transactions): Make profiler queryable (#6122) by @Zylphrex
- bug(mql): Fix MQL totals queries (#6125) by @enochtangg
- chore: update some readmes related to migrations and autogen (#6076) by @kylemumma
- ref(lwdeletes): create parse_and_run_query function (#6120) by @volokluev
- feat(profiling): Write profiler_id to transactions table (#6111) by @Zylphrex
- Give telemetry experience team access to cardinality analyzer (#6115) by @vgrozdanic
- Add an inner join relationship for search_issues and group_attributes (#6119) by @snigdhas
- support max_bytes_before_external_group_by ClickHouse insert flag (#6118) by @onewland
- fix(mql): Nested filter conditions (#6117) by @john-z-yang
- code (#6114) by @xurui-c
- code (#6113) by @xurui-c
- chore: cleanup SEARCH_ISSUES_TMP storage set (#6110) by @kylemumma
- code (#6105) by @xurui-c
- chore: make snuba co-owners of datasets again (#6104) by @kylemumma
- fix: LowCardinality column bug w/ aliases (#6108) by @MeredithAnya
- meta: Bump new development version (5dc9763c)

_Plus 2 more_

## 24.7.0

### Various fixes & improvements

- feat(mql): MQL JOIN subquery generator (#6101) by @enochtangg
- feat(metrics): add wildcard support to MQL parser (#5972) by @shellmayr
- Revert "feat(profiling): Add profiler id column to transactions (#6099)" (f941b5e7) by @getsentry-bot
- feat(profiling): Add profiler id column to transactions (#6099) by @Zylphrex
- feat: create search_issues_dist_v2 on same query node as group_attributes (#6087) by @kylemumma
- feat(replays): Set platform as empty string if platform is not provided (#6098) by @cmanallen
- feat(mql): Add test for formula queries with curried aggregate (#6080) by @john-z-yang
- feat(mql): Arbitrary nested column aggregates (#6096) by @john-z-yang
- dev(rate-limits): bump backfill query allocation to 30 per s (#6074) by @JoshFerge
- ref(snuba-deletes): Skeleton for DeletionProcessor (#6095) by @xurui-c
- ref(snuba-deletes): add DeletionSettings (#6092) by @MeredithAnya
- fix: move an import that may be causing issue with sentry devservice (#6090) by @kylemumma
- Revert "code (#6088)" (#6089) by @kneeyo1
- fix(capman): emit the bytes scanned metric from db_query (#6075) by @volokluev
- code (#6088) by @xurui-c
- feat(mql): Support groupby parsing for formula join queries (#6077) by @enochtangg
- Revert "code (#6081)" (ba3e5ed3) by @getsentry-bot
- code (#6081) by @xurui-c
- Add clickhouse override settings (#6085) by @cmanallen
- chore: new temporary storage set (#6079) by @kylemumma
- feat(replays): Add materialization YAML definitions (#6071) by @cmanallen
- fix(devenv): Auto-install cargo watch (#6078) by @untitaker
- feat: autogenerate addcolumn migrations (#6053) by @kylemumma
- test(mql): Add tests for formula queries with scalar values (#6073) by @john-z-yang

_Plus 29 more_

## 24.6.0

### Various fixes & improvements

- chore(on-call): Properly handling query validation errors (#6019) by @enochtangg
- update CH version used in CI (#6027) by @volokluev
- code (#6026) by @xurui-c
- code (#6024) by @xurui-c
- doc(rs): some docs for the ClickHouse batch module (#6020) by @onewland
- chore(gocd): Bumping gocd-jsonnet version (#6021) by @IanWoodard
- [CapMan] change default throttle values (#6022) by @xurui-c
- [CapMan] Add throttling to ReferrerGuardRail policy (#6014) by @xurui-c
- feat(issue-search): Add group_first_release_id to GroupAttributes processor (#5987) by @snigdhas
- Fixing the metric increment for cache hit/miss and (#6018) by @nachivrn
- ref(ci): update clickhouse versions (#6016) by @MeredithAnya
- update docs for snuba storage queries (#6013) by @enochtangg
- In the Snuba Admin Production queries UI (#6015) by @nachivrn
- SNS-2737: Making changes for a simple readthrough cache without queuing (#5992) by @nachivrn
- chore(profiles-chunks): Add missing consumer to deployment list (#6001) by @rgibert
- fix(gocd): Updating canary checks to happen after the deploy goes out (#6004) by @IanWoodard
- ref(admin): Add a system query for data skipping indexes (#6008) by @evanh
- feat(issue-search): Add group_first_release_id to the GroupAttributes table (#5986) by @snigdhas
- ref: sentry-kafka-schemas 0.1.90 (#6007) by @lynnagara
- ref(async-inserts): add clickhouse-concurrency cli arg (#5999) by @MeredithAnya
- fix: snuba admin queries check (#6005) by @MeredithAnya
- add tags[environment] to discover low cardinality processor (#5997) by @enochtangg
- meta: Bump new development version (9dac11b7)

## 24.5.1

### Various fixes & improvements

- chore(gocd): Bumping gocd-jsonnet version (#6000) by @IanWoodard
- ref(async-inserts): Add system queries (#5998) by @MeredithAnya
- Include metrics for the number of queries resulting in cache hits, misses, and waits associated with datasets. (#5991) by @nachivrn
- fix(meta) Change the view to use LEFT ARRAY JOIN (#5975) by @evanh
- Revert "add option to print full payload on invalid message (#5979)" (#5981) by @john-z-yang
- fix(error): Skip lineno deserialization on failure (#5983) by @john-z-yang
- feat(generic-metrics): Add gauges subscriptions consumers to Snuba deployment (#5980) by @ayirr7
- add option to print full payload on invalid message (#5979) by @john-z-yang
- chore: retire use_new_combine_conditions feature flag (#5977) by @kylemumma
- ref(subscriptions): Add query-based exception handling in scheduler  (#5976) by @ayirr7
- SNS 2645 - Surface allocation policy decisions in production queries tool (#5970) by @nachivrn
- remove logging (#5978) by @xurui-c
- fix(admin): Allow LEFT ARRAY JOIN in the admin tool (#5974) by @evanh
- feat(replays): Add migration for distributed materialization (#5951) by @cmanallen
- ref(async-inserts): allow async inserts as an option (#5955) by @MeredithAnya
- allow one replacement project to be skipped (#5965) by @xurui-c
- Debugging bucket timer (#5963) by @xurui-c
- feat(generic-metrics): Add logic in distributions processor to drop percentiles (#5911) by @ayirr7
- feat(generic-metrics): Add migrations for opt-in disabled percentiles (#5910) by @ayirr7
- fix(clickhouse): Remove unecessary logging (#5958) by @nikhars
- logging (#5959) by @xurui-c
- feat(metrics): add metric_stats use case to generic metrics meta tables (#5954) by @shellmayr
- number of projects skipped (#5952) by @xurui-c
- fix(snuba-admin): Add quotes around ColumnSizeOnDisk's table value (#5953) by @phacops

_Plus 28 more_

## 24.5.0

### Various fixes & improvements

- ref(rust-consumer): join-timeout per-step (#5918) by @untitaker
- ref: Bump arroyo (#5917) by @untitaker
- ref(api): parse pipeline tests, mql after treeify #5894 (#5894) by @kylemumma
- one more auto-offset-reset earliest (#5909) by @lynnagara
- fix: Temporarily remove check for coupled migrations and code changes (#5903) by @ayirr7
- ref: bump sentry-kafka-schemas to 0.1.82 (#5906) by @getsentry-bot
- fix(CI): Make timestamp relative for metrics summaries test  (#5904) by @ayirr7
- feat(generic-metrics): Retry adding gauges subscriptions topics + configs (#5892) by @ayirr7
- ref(api): parse pipeline tests, snql pipeline before treeify (#5886) by @kylemumma
- sessions: drop the clickhouse tables (#5882) by @lynnagara
- feat: Add support for DateTime64 column type (#5896) by @phacops
- fix: dlq-consumer should default to auto-offset-reset earliest (#5893) by @lynnagara
- fix(capman): make allocation policies work with joins (#5887) by @volokluev
- ref(devexp): Make entities optional in dataset config (#5879) by @enochtangg
- ref(api): parse pipeline tests, mql pipeline before treeify (#5885) by @kylemumma
- bump snuba-sdk to 2.0.34 (#5884) by @enochtangg
- chore(self-hosted): Remove dependency on snuba-image build step during e2e action (#5891) by @hubertdeng123
- remove skip-write flag (#5846) by @john-z-yang
- feat(devexp): Add required_time_column to storage (#5889) by @volokluev
- feat(spans): Add indexes for tag columns (#5871) by @phacops
- feat(generic-metrics): Add zstd decompression to generic metrics processor (#5845) by @ayirr7
- snuba devserver fix? (#5873) by @MeredithAnya
- update CLICKHOUSE_SERVER_MIN_VERSION and CLICKHOUSE_SERVER_MAX_VERSION (#5853) by @MeredithAnya
- fix: Handle tag columns in low cardinality processor (#5875) by @evanh

_Plus 35 more_

## 24.4.2

### Various fixes & improvements

- feat(generic-metrics): Add success metric around Base64 message processing (#5830) by @ayirr7
- chore(deps): bump python and node (#5829) by @mdtro
- ref: remove pushing to legacy gcr (#5819) by @asottile-sentry
- Enables Clickhouse Authorization (#5818) by @xurui-c
- remove ConditionSimplifierProcessor (#5820) by @volokluev
- chore(dev-exp): Remove old pipeline code (#5821) by @enochtangg
- Revert "feat(generic-metrics): Add support for subscriptions to gauges (#5736)" (d0c50931) by @getsentry-bot
- feat(generic-metrics): Add Base64 decoding to Snuba processors (#5761) by @ayirr7
- feat(generic-metrics): Add support for subscriptions to gauges (#5736) by @ayirr7
- add a metric for how many queries are waiting for readthrough cache (#5817) by @volokluev
- ref: use artifact registry (#5816) by @asottile-sentry
- feat(meta): Create distributions meta tables (#5748) by @evanh
- chore: Bump Kafka schema version to 0.1.71 (#5814) by @ayirr7
- Remove unnecessary test (#5812) by @ayirr7
- feat(meta): Create gauges meta tables (#5749) by @evanh
- Revert "Revert "ref: dual-write docker image to artifact registry (#5798)"" (#5813) by @volokluev
- fix: Don't log this every time, it clogs the logs up (#5799) by @evanh
- feat(devexp): Split entity and storage processing for composite queries (#5785) by @enochtangg
- feat(meta) Create sets meta tables (#5747) by @evanh
- Emit a metric when replacements are skipped (#5809) by @xurui-c
- Revert "ref: dual-write docker image to artifact registry (#5798)" (b69f85f5) by @getsentry-bot
- ref: dual-write docker image to artifact registry (#5798) by @asottile-sentry
- fix(meta) Rename counter tables for consistency (#5797) by @evanh
- fix(22.8) don't use ifnull for cardinality casting (#5807) by @volokluev

_Plus 14 more_

## 24.4.1

### Various fixes & improvements

- feat(replays): add migration for replay_id in discover (#5790) by @JoshFerge
- feat: replacer defaults to auto.offset.reset=earliest (#5772) by @lynnagara
- feat(migrations): add discover local (#5788) by @dbanda
- fix(spans): Let null domain be null (#5780) by @phacops
- feat(meta): Adjust partitioning/settings of counters meta tables (#5784) by @evanh
- Revert "feat(replays): add replay_id column to merged discover table (#5777)" (2f1509fc) by @getsentry-bot
- fix: fix mypy --strict vscode (#5781) by @kylemumma
- feat(replays): add replay_id column to merged discover table (#5777) by @JoshFerge
- ref: Rust consumer should not skip writes by default (#5778) by @lynnagara
- chore(on-call): Add CrossOrgQueryAllocationPolicy to errors (#5774) by @enochtangg
- inc-715: rust consumer can stop processing messages at specific timestamp (#5779) by @lynnagara
- meta: Bump new development version (2248bb55)
- fix(meta): Remove experimental meta tables (#5733) by @evanh
- ref(card-an): Allow the new meta tables in the cardinality analyzer (#5769) by @evanh

## 24.4.0

### Various fixes & improvements

- chore(on-call): Add metric for concurrent queries by referrer that violate policy (#5767) by @enochtangg
- ref: make auto.offset.reset=earliest everywhere (#5765) by @lynnagara
- feat(trace): Add trace id to transactions (#5768) by @wmak
- fix(spans): Move span id above trace id in the prewhere (#5766) by @wmak
- feat(meta): Add record meta column to gauges (#5760) by @evanh
- feat(meta): Add record meta column to distributions (#5759) by @evanh
- feat(meta): Add record_meta column to sets (#5735) by @evanh
- ref: bump sentry-kafka-schemas to 0.1.68 (#5764) by @getsentry-bot
- chore(capman): set default bytes scanned limit for rejecting policy (#5755) by @volokluev
- ref(ch-upgrades): create dist tables functionality (#5737) by @MeredithAnya
- perf(metrics): Use kafka header optimization (#5756) by @nikhars
- feat(meta): Add updated versions of meta tables for counters (#5734) by @evanh
- chore(rust): Update dependencies (#5751) by @nikhars
- chore: update sdk version (#5754) by @kylemumma
- fix: Fix default auto-offset-reset value (#5753) by @lynnagara
- lower max query size to 128KiB (#5750) by @enochtangg
- clean up old simple pipeline (#5732) by @enochtangg
- feat(capman): Long term rejection allocation policy (#5718) by @volokluev
- ref(fetcher): --tables optional now (#5730) by @MeredithAnya
- fix: parser, bug in pushdown filter (#5731) by @kylemumma
- feat(generic-metrics): Add a killswitch to processor (#5617) by @ayirr7
- feat(replays): Replace python processor with a rust-based processor (#5380) by @cmanallen
- feat(replay): add ReplayViewedEvent to replay processor (#5712) by @aliu3ntry
- chore(deps): bump h2 from 0.3.22 to 0.3.26 in /rust_snuba (#5727) by @dependabot

_Plus 52 more_

## 24.3.0

### Various fixes & improvements

- Unrevert: feat: Remove query splitters from the API  (#5581) by @evanh
- feat: Add use_case_id index to generic metrics (#5655) by @evanh
- ref(ci): Remove deleted test file (#5656) by @evanh
- fix vscode debugger (#5652) by @kylemumma
- chore: Upgrade snuba-sdk to 2.0.31 (#5647) by @iambriccardo
- fix(gocd): put snuba cmd into $SNUBA_CMD (#5654) by @MeredithAnya
- enable canary health check (#5649) by @enochtangg
- Revert "fix(CapMan): Allocation Policies causing potentially timeout errors on ST (#4403)" (703042e1) by @getsentry-bot
- fix(gocd): add SNUBA_CMD_TYPE (#5648) by @MeredithAnya
- Allows empty `trace_id` (#5637) by @xurui-c
- fix: Fix bump version for rust (#5643) by @lynnagara
- feat(generic-metrics): Add metrics around encoding format type in processor (#5627) by @ayirr7
- feat: filter by metric_id in select logical query optimizer (#5610) by @kylemumma
- fix(gocd): fix unbound variable (#5641) by @MeredithAnya
- ref: bump sentry-kafka-schemas to 0.1.60 (#5642) by @getsentry-bot
- add canary health check to gocd pipeline (#5638) by @enochtangg
- ref(codecov) Try out the failed test feature in Codecov (#5635) by @evanh
- feat(spans): Enable spans storage in ST and self-hosted (#5629) by @phacops
- fix: Fix a bug in HexIntColumnProcessor that skipped array conditions (#5640) by @evanh
- ref(gocd): use shared script query-fetcher (#5639) by @MeredithAnya
- ref(gocd): add comparer pipeline, consolidate script? (#5636) by @MeredithAnya
- feat(spans): Set the migration group as complete to run migrations everywhere (#5634) by @phacops
- feat(admin): Absolute imports in snuba-admin (#5630) by @volokluev
- the default value of trace_id will be a randomly generated uuid inste… (#5628) by @xurui-c

_Plus 72 more_

## 24.2.0

### Various fixes & improvements

- Revert "feat(rust): Count schema validation failures (#5515)" (b597627c) by @getsentry-bot
- ref(gcs): add list_blobs and blob_exists (#5545) by @MeredithAnya
- add referrer guard rail policy to every storage (#5536) by @volokluev
- ref: Remove PolymorphicMetricsProcessor (#5541) by @untitaker
- fix(rust): Use the same release as in Python (#5543) by @untitaker
- ref: Make ExceptionMechanism nullable (#5542) by @untitaker
- feat: Add team-ops to snuba admin (#5540) by @lynnagara
- ref(rust): Remove concurrency override (#5538) by @lynnagara
- ref: Remove unused topics (#5537) by @lynnagara
- cleanup(capman): remove legacy rate limit configs (#5513) by @volokluev
- fix(spans): Profile id need UUIDColumnProcessor in indexed spans (#5533) by @Zylphrex
- ref: bump sentry-kafka-schemas to 0.1.49 (#5534) by @getsentry-bot
- fix(rust): Nullability around headers and frames (#5532) by @untitaker
- fix(gocd): remove explicit context flag from snuba-stable (#5529) by @bmckerry
- fix(rust): Fix a few more nullability issues (#5531) by @untitaker
- fix(rust-errors): Make stacktrace nullable as well (#5527) by @untitaker
- change: Move curl from buildDep to runtimeDep (#5528) by @dmajere
- fix: xfail optimize test again :( (#5506) by @MeredithAnya
- feat(querylog): Add query to get project scanning most bytes (#5530) by @nikhars
- fix(metrics): Update gauges to handle avgIf functions (#5523) by @evanh
- fix(querylog): Use _dist table (#5526) by @nikhars
- build(deps): bump sentry-arroyo from 2.16.0 to 2.16.1 (#5516) by @dependabot
- feat(rust): Count schema validation failures (#5515) by @phacops
- feat(mql): Parse the entity from the MRI (#5501) by @evanh

_Plus 4 more_

## 24.1.2

### Various fixes & improvements

- feat(generic-metrics): Add DLQ back to generic metrics storages (#4964) by @ayirr7
- feat(rust): Generic metrics compat processor (#5469) by @lynnagara
- ref(rust): Add better error messages (#5508) by @untitaker
- Reapply "ref: Python 3.11.6 (#5476)" (#5505) by @getsentry-bot
- fix(py311): Explicitly import importlib.abc (#5507) by @untitaker
- Revert "ref: Python 3.11.6 (#5476)" (#5505) by @volokluev
- feat(capman): Referrer guard rail policy (#5481) by @volokluev
- ref: Python 3.11.6 (#5476) by @untitaker
- Bumping GoCD jsonnet to include de (#5493) by @IanWoodard
- fix(rust): Work around poor error messages by serde (#5503) by @untitaker
- Tags can be submitted as null (#5504) by @cmanallen
- fix(capman): Fix invalid tenant ids exception in allocation policy (#5489) by @volokluev
- admin nav bug fix (#5499) by @kylemumma
- feat: Deploy release health rust consumer (#5500) by @lynnagara
- ref: Add rust-errors-consumer to deploys, remove a bunch of already-removed deployments (#5478) by @untitaker
- fix(cardinality): Fix the query run for cardinality (#5498) by @nikhars
- ref(ddm): Disable metrics in Snuba (#5497) by @evanh
- fix: Use the working link to the MQL repository (#5495) by @olksdr
- Take 2: Port PolymorphicMetricsProcessor to Rust (#5492) by @ayirr7
- Revert "Revert "Revert "feat(rust): Port PolymorphicMetricsProcessor to Rust (#5419)""" (#5491) by @john-z-yang
- Revert "Revert "feat(rust): Port PolymorphicMetricsProcessor to Rust (#5419)"" (7446c1ad) by @john-z-yang
- Revert "feat(rust): Port PolymorphicMetricsProcessor to Rust (#5419)" (871e509f) by @getsentry-bot
- feat(rust): Port PolymorphicMetricsProcessor to Rust (#5419) by @ayirr7
- fix(ddm): Fix config bug (#5488) by @evanh

_Plus 28 more_

## 24.1.1

### Various fixes & improvements

- fix(inc-602): make migration connection checks readiness-state aware (#5414) by @volokluev
- chore(entity): Remove runtime config for illegal aggregation validator (#5443) by @enochtangg
- fix(gcb): Remove build steps from GCB (#5450) by @untitaker
- build(deps): bump sentry-sdk from 1.26.0 to 1.39.2 (#5366) by @dependabot
- fix: Bump sentry-kafka-schemas in right project (#5451) by @untitaker
- ref(optimize): Use time-machine instead of freezegun (#5447) by @nikhars
- chore(deps): bump sentry-relay from 0.8.39 to 0.8.44 (#5442) by @dependabot
- fix(devserver): Enforce schemas again (#5449) by @untitaker
- fix: Upgrading to Clickhouse 22.8 (#5445) by @evanh
- fix(devserver): Do not enforce schema (#5446) by @untitaker
- fix(cardinality): Add filter to cardinality report (#5439) by @nikhars
- fix(optimize): Add parallel optimize job cutoff time (#5444) by @nikhars
- feat(spans): Add a schema test (#5440) by @phacops
- feat(profiles): Add test to see if the struct matches the schema (#5438) by @phacops
- ref: bump sentry-kafka-schemas to 0.1.46 (#5441) by @getsentry-bot
- feat(on-call): Add illegal aggregate function in conditions entity validator (#5435) by @enochtangg
- ref(docs) Add some MQL documentation to Snuba (#5432) by @evanh
- feat(metrics-summaries): Handle incomplete metrics summaries (#5428) by @phacops
- fix config validation for cross org policy (#5426) by @volokluev
- fix(rust): Deal with empty batches correctly (#5433) by @untitaker
- ref(replays): Re-add e2e latency metric [SNS-2606] (#5420) by @untitaker
- fix(rust): Fix broken DLQ produer in Rust (#5431) by @untitaker
- fix(cardinality): Increase time duration for finding all modules (#5430) by @nikhars
- feat(gcs): Allow snuba to write to gcs for tooling (#5410) by @nikhars

_Plus 32 more_

## 24.1.0

### Various fixes & improvements

- fix(rust): Actually drop before exiting (#5394) by @untitaker
- ref(rust): Bump cogs accountant library (#5391) by @lynnagara
- fix(mql): Properly encode/decode double quotes in MQL strings (#5338) by @evanh
- fix(rust-consumers): Define consumer group for functions consumer (#5389) by @Zylphrex
- ref(rust): Don't panic in RunTaskInThreads::poll (#5387) by @loewenheim
- deps(rust): Change rdkafka dep to upstream master (#5386) by @loewenheim
- fix(rust): Don't produce commit log in --skip-write mode (#5385) by @lynnagara
- feat(spans): add migration to add compression to spans (#4726) by @dbanda
- feat: Add snapshot tests for processors (#5379) by @untitaker
- ref(metrics): Refactor how global tags work, and introduce min_partition tag (#5346) by @untitaker
- fix(devenv): Make post-merge hook exactly like sentry's (#5378) by @untitaker
- fix(Makefile): Change ordering for Rust dev setup (#5384) by @ayirr7
- Validate at least one event link was sent (#5383) by @cmanallen
- ref: speed up .github/workflows/image.yml (#5382) by @asottile-sentry
- feat(rust): Port generic metrics to rust consumer (#5360) by @nikhars
- fix(rust): Remove noise from output of make install-rs-dev (#5373) by @untitaker
- feat(spans): Set origin_timestamp for spans (#5372) by @phacops
- feat(rust): Cogs recording utility for generic metrics (#5362) by @lynnagara
- fix(ci): Add ddm_meta test to "full tests" (#5370) by @untitaker
- feat(MQL): Bump snuba-sdk to 2.0.18 and support arbitrary functions in MQL parser (#5358) by @enochtangg
- fix(ci): Wait for healthcheck to succeed before running test_distributed (#5363) by @untitaker
- ref(docker): Remove obsolete build args (#5369) by @untitaker
- Revert "feat(spans): Set origin_timestamp for spans (#5367)" (1c963c3e) by @getsentry-bot
- ref: Remove experimental consumers from gocd deployment (#5368) by @lynnagara

_Plus 108 more_

## 23.12.1

### Various fixes & improvements

- perm(cardinality-analyzer): Replace team-ingestion-pipeline with team-ingest (#5240) by @Dav1dde
- ref(profiling): add profiles to admin and web (#5234) by @MeredithAnya
- feat(replays): Add react_component_name column (#5232) by @cmanallen
- feat(cardinality): Add clickhouse host and port parameters (#5237) by @nikhars
- feat(replays): Add react_component_name column migration (#5231) by @cmanallen
- chore(deps): bump @sentry/react from 7.56.0 to 7.88.0 in /snuba/admin (#5224) by @dependabot
- Optimize ConsumerState (#5236) by @Swatinem
- fix(rust): Handle rebalancing ourselves, remove usage of pre/post-rebalance callbacks (#5229) by @untitaker
- Unify Timeout/Deadline operations (#5230) by @Swatinem
- feat(rust): Port arroyo.consumer.latency metric (#5214) by @loewenheim
- feat(metrics) Add a MQL endpoint to Snuba API  (#5193) by @evanh
- fix(rust): Fix consumer deadlock on strategy panic (#5216) by @lynnagara
- ref(migrations): add experimental readiness state to policies (#5206) by @MeredithAnya
- Use jemalloc unconditionally (#5228) by @Swatinem
- feat(replays): Add rust-based message processor (#5215) by @cmanallen
- Use jemalloc in Docker image (#5227) by @Swatinem
- fix(rust): Fix `self.consumer_offsets` (#5220) by @lynnagara
- build(rust): Update maturin and add to requirements.txt (#5218) by @lynnagara
- feat(metrics-summaries): Deploy metrics-summaries consumer automatically (#5221) by @phacops
- feat(metrics-summaries): Enable metrics-summaries for SaaS (#5219) by @phacops
- feat(rust): Port run_task_in_threads test (#5213) by @loewenheim
- meta: Bump new development version (b18bf981)

## 23.12.0

### Various fixes & improvements

- fix(arroyo): Tag all consumer metrics by the smallest partition index (#5208) by @untitaker
- feat(metrics-summaries): Add a Rust consumer for metrics summaries (#5210) by @phacops
- fix(slo): Add ILLEGAL_COLUMN = 44 as an invalid request (#5209) by @evanh
- ref(rust): Give BufferedMessages a maximum (#5211) by @loewenheim
- ref(admin): Add AllMigrations role (#5205) by @MeredithAnya
- fix(rust): Separate clickhouse concurrency from processing (#5207) by @untitaker
- fix(rust): Increase poll interval (#5197) by @lynnagara
- feat(rust): Implement LocalProducer and port a test (#5202) by @loewenheim
- fix(metrics-summaries): Remove low_cardinality modifier as the column is not low cardinality (#5204) by @phacops
- feat(guages): Add `avg` capability to guages (#5196) by @volokluev
- fix(rust): Consumer should crash if DLQ limit is reached (#5187) by @lynnagara
- ref: Remove stale querylog deployments (#5201) by @lynnagara
- fix(cache): Slightly clean up cache set errors (#5199) by @evanh
- perf: Increase consumer join timeout (#5200) by @lynnagara
- fix(rust): Fix SLO metrics again again again [SNS-2565] (#5198) by @untitaker
- ref(metrics-summaries): Skip all the work if the field needed is not there (#5194) by @phacops
- feat(spans): Store metrics_summary in the proper column if needed (#5191) by @phacops
- fix(spans): Add sentry_tags to spans entity (#5162) by @nikhars
- perf: Reusable multiprocessing pools v2 (#5192) by @lynnagara
- fix(rust): Suppress error when manually resetting offsets (#5186) by @untitaker
- fix(rust): Do not drop runtime and accidentally cancel tasks (#5189) by @untitaker
- feat(spans): Add a metrics summary processor (#5161) by @phacops
- feat(spans): Add a metrics summary column to link spans with DDM metrics (#5159) by @phacops
- fix(metrics_summaries): Fix sample by expression (#5190) by @phacops

_Plus 62 more_

## 23.11.2

### Various fixes & improvements

- ref(rust): Add a few metrics missing from snuba dashboard (#5102) by @untitaker
- ref(rust): Put back the querylog deployment (#5101) by @lynnagara
- fix(rust): Capture warnings as exceptions (#5100) by @loewenheim
- fix(rust): Untangle offset commit on revocation (#5095) by @loewenheim
- fix(discover): Enable column validator on discover entities (#5094) by @evanh
- fix: Skip warnings in gocd error checks (#5091) by @evanh
- feat(rust): Populate commit_log_offsets so they can be produced (#5086) by @lynnagara
- fix(rust-python): Join handles less frequently, fix backpressure bug (#5088) by @untitaker
- fix(validation): Add missing replay columns (#5090) by @evanh
- Add profile_id to spans query columns (#5087) by @enochtangg
- feat(rust): Add snuba latency SLO metric (#5071) by @untitaker
- fix: make sure SNUBA_RELEASE is set (#5085) by @MeredithAnya
- ref: bump sentry-arroyo to 2.14.22 (#5089) by @getsentry-bot
- fix(metrics) Add _indexed_tags_hash column to distributions (#5081) by @evanh
- Add a benchmark to test Rust consumers (#5024) by @Swatinem
- meta: Bump new development version (8b263f1c)
- fix(rust): Fix the ProduceCommitLog strategy (#5076) by @lynnagara
- fix(replacegroup): Catch bad datetime formats in ReplaceGroup (#5083) by @evanh
- feat(self-hosted): disable allocation policy in self hosted (#5084) by @volokluev

## 23.11.1

### Various fixes & improvements

- fix(validation): EntityContainsColumnValidator wasn't actually enabled (#4399) by @rahul-kumar-saini
- ref: bump sentry-arroyo to 2.14.21 (#5079) by @getsentry-bot
- feat(rust): Add building blocks for DLQ (#5072) by @lynnagara
- fix: Remove unnecessary locking in metrics (#5077) by @untitaker
- ref(rust): Ensure none keys are properly stripped from broker config (#5075) by @lynnagara
- ref: Temporarily remove querylog rust consumer from gocd deployment (#5073) by @lynnagara
- fix(rust): Kafka headers improvements (#5063) by @lynnagara
- fix(rust): Clear all backpressure state between assignments (#5055) by @untitaker
- ref: Bump sentry-kafka-schemas to 0.1.35 (#5062) by @lynnagara
- Revert "feat: Experiment using statsdproxy for aggregation (#4734)" (201e07b0) by @getsentry-bot
- Relicense under FSL-1.0-Apache-2.0 (#5058) by @chadwhitacre
- add raw tags hash column to sets dist table (#5059) by @enochtangg
- ci: Reduce Sentry test concurrency (#5061) by @lynnagara
- fix(rust): Rename run_once metric (#5056) by @untitaker
- feat(rust): Buffered messages option 3 (#5053) by @lynnagara
- feat: Experiment using statsdproxy for aggregation (#4734) by @lynnagara
- fix(rust): Rename metrics namespace, and implement one metric from python (#5036) by @untitaker
- skip flake test_optimized_partition_tracker unit test (#5052) by @enochtangg
- fix(generic-metrics): Add raw tags hash to gauges dist table (#5049) by @enochtangg
- Add support for join queries to the subscription system (#5006) by @wedamija
- ref: bump sentry-arroyo to 2.14.20 (#5047) by @getsentry-bot
- Spawn only one Tokio Runtime (#5039) by @Swatinem
- Avoid double locking (#5048) by @Swatinem
- ref(rust): Add --python-max-queue-depth option (#5030) by @untitaker

_Plus 11 more_

## 23.11.0

### Various fixes & improvements

- Move `SchemaValidator` into async task  (#5037) by @Swatinem
- Micro-optimize `ClickhouseWriter` (#5035) by @Swatinem
- ref(rust): Add source information to `RunError` (#5034) by @loewenheim
- fix(cli): Remove misleading defaults (#4837) by @untitaker
- Remove `BadMessage` in favor of `anyhow::Error` (#5032) by @Swatinem
- Remove some unused Send/Sync bounds (#5029) by @loewenheim
- Use more early-returns (#5023) by @Swatinem
- ref(rust): Fix inconsistency in metrics trait (#5027) by @lynnagara
- feat(rust): Add strategy that produces the commit log topic (#4976) by @lynnagara
- chore(rust): Move common functionality out (#5026) by @nikhars
- ref(rust): Rename Transform to RunTask (#5021) by @lynnagara
- Avoid Mutex on `AssignmentCallbacks` (#5025) by @Swatinem
- ref: Remove most Clone bounds (#5020) by @loewenheim
- feat(rust): Port the buffered messages class to Rust (#4993) by @lynnagara
- fix(migration): move metrics migration into the right place (#5007) by @volokluev
- feat(settings): Allow runtime configurable referrer overrides (#5005) by @nikhars
- Optimize subset checks (#5018) by @Swatinem
- ref: Rename and simplify TopicContent struct (#5017) by @loewenheim
- Intern `Topic` and make it `Copy` (#5016) by @Swatinem
- Avoid intermediate `HashMap` in TopicPartitionList (#5015) by @Swatinem
- Give `Topic` and `Partition` a `new` fn (#5014) by @Swatinem
- Apply clippy suggestions (#5008) by @Swatinem
- ref: rust-rdkafka 0.36 (#5004) by @lynnagara
- fix(metrics): add _raw_tags_hash column to distributed table (counters) (#4998) by @volokluev

_Plus 75 more_

## 23.10.1

### Various fixes & improvements

- feat(cardinality): Cardinalily analysis reporting (#4893) by @nikhars
- ref(rust): Skip procspawn::init() if pure rust (#4899) by @lynnagara
- fix(rust): Fix the consumer pause condition when backpressure happens (#4898) by @lynnagara
- fix(clickhouse-v23.3): Fix function tuple can't have lambda expression as arguments bug (#4853) by @enochtangg
- feat(subscriptions): Record received_p99 - take 2 (#4894) by @lynnagara
- fix: fix 0001_functions and 0001_querylog for clickhouse 23 (#4842) by @untitaker
- feat: Squash errors migrations (#4854) by @lynnagara
- Revert "feat: Write received_p99 to commit log (#4872)" (2d3fc182) by @getsentry-bot
- feat: Write received_p99 to commit log (#4872) by @lynnagara
- feat(rust): Update all dependencies in lockfile (#4892) by @lynnagara
- feat(generic-metrics): Add gauges storage set locally (#4888) by @ayirr7
- deps: bump node to 20.8.1 (#4884) by @mdtro
- feat(CoGS): Record bytes scanned for Generic Metrics queries by use case (#4748) by @rahul-kumar-saini
- ref: bump sentry-arroyo to 2.14.13 (#4887) by @getsentry-bot
- lint(generic-metrics): Remove feature flag for gen-metrics counters mat view version (#4891) by @john-z-yang
- feat(subscriptions): Add mechanism for storage to define timestamp used for scheduling (#4873) by @lynnagara
- configure dependabot and dependency review (#4885) by @mdtro
- fix: Log step name in run_task_in_threads (#4875) by @untitaker
- bump to debian 12 and configure dependabot for docker + gh actions (#4874) by @mdtro
- ref(23.3) Fix test so it runs on 23.3 (#4863) by @evanh
- feat(generic-metrics): Bump `materialization_version` for generic sets metrics to 2 (#4869) by @john-z-yang
- feat: Make subscription scheduler invalid interval a metric not warning (#4855) by @lynnagara
- feat(generic-metrics): Add new mat view for generic counter metrics (#4867) by @john-z-yang
- lint(generic-metrics): Remove feature flag for gen-metrics sets mat view version (#4866) by @john-z-yang

_Plus 1 more_

## 23.10.0

### Various fixes & improvements

- feat(slack): Allow sending files in slack client (#4865) by @nikhars
- ref: Simplify collector / processed message batch writer (#4848) by @lynnagara
- deploy static experimental consumers with statefulsets (#4856) by @dbanda
- feat: Mark first migration blocking (#4849) by @lynnagara
- deploy to experimental static membership consumers (#4851) by @dbanda
- feat: Reduce more logging (#4852) by @lynnagara
- test: Run full Sentry test suite if any migrations changed (#4850) by @lynnagara
- feat(generic-metrics): Bump `materialization_version` for generic sets metrics to 2 (#4820) by @john-z-yang
- feat(generic-metrics): Add new mat view for generic set metrics (#4803) by @john-z-yang
- feat(generic-metrics): Write `retention_days` to `min_retention_days` columns for sets and counters (#4819) by @john-z-yang
- docs(migrations): document --fake (#4844) by @MeredithAnya
- remove `--force` from ST migrations (#4840) by @dbanda
- Add 10s granularity to generic metrics granularity processors (#4834) by @ayirr7
- fix: Fix rust logging setup and sentry integration (#4843) by @untitaker
- Upgrade Python to 3.8.18 (#4841) by @oioki
- feat: Reduce logging in Rust consumers (#4845) by @lynnagara
- fix(slack): Allow slack to talk to different channels. (#4810) by @nikhars
- fix(dlq): Add sane defaults to the dlq policy (#4817) by @nikhars
- fix(cardinality-analyzer): Add more storage keys to cardinality analyzer (#4839) by @john-z-yang
- fix: Fix committing in the no-skip-write python consumer (#4838) by @lynnagara
- ref(spans): Refactor spans Rust processor (#4833) by @phacops
- fix(spans): Handle null and no tags values (#4830) by @phacops
- ref: bump sentry-arroyo to 2.14.12 (#4829) by @getsentry-bot
- fix: deploy new dummy rust consumers (#4828) by @untitaker

_Plus 65 more_

## 23.9.1

### Various fixes & improvements

- feat(spans): Port the processor to Rust (#4712) by @john-z-yang
- meta: Bump new development version (2432b27d)

## 23.9.0

### Various fixes & improvements

- feat(rust): Add processor metrics for Rust consumer (#4737) by @lynnagara
- feat: metrics.increment() no longer supports option (#4738) by @lynnagara
- ref: bump sentry-kafka-schemas to 0.1.27 (#4739) by @getsentry-bot
- Update snuba-sdk version to latest 2.0.1 (#4735) by @enochtangg
- make sure possible API changes are tested against sentry (#4736) by @volokluev
- test: Run different Sentry tests depending on files changed (#4727) by @lynnagara
- fix(ci): no more sentry shards (#4725) by @joshuarli
- fix(CI): Unblock deploys as they hard check for sentry matrix'd tests (#4724) by @rahul-kumar-saini
- fix(on-call): Negative org ID in Tenant IDs crashes Querylog consumer (#4722) by @rahul-kumar-saini
- ref: bump sentry-kafka-schemas to 0.1.26 (#4721) by @getsentry-bot
- test: No need to run Sentry CI in a matrix (#4720) by @lynnagara
- Use snuba-admin spec for ST and SaaS (#4716) by @dbanda
- feat(rust-arroyo): Processor calls consumer.poll() with timeout of 1 second (#4719) by @lynnagara
- experiment(on-call): Profile bytes scanned potentially incorrect (#4714) by @rahul-kumar-saini
- feat(reduce): Avoid clone and ensure all messages are processed in join (#4718) by @lynnagara
- improvement(visibility): make the replacer log project_ids for slow replacements (#4717) by @onewland
- feat(profiling): Add a functions processor in Rust (#4705) by @phacops
- fix(spans): Handle bad group and group_raw values (#4715) by @phacops
- feat(profiling): Add a profiles processor in Rust (#4698) by @phacops
- Remove canary deploys in non-US regions (#4696) by @mattgauntseo-sentry
- feat(spans): write tags to sentry_tags col (#4706) by @dbanda
- sort lists before compare (#4711) by @dbanda
- fix(rust-arroyo): Fix timeout in RunTaskInThreads::join() (#4704) by @lynnagara
- ref(rust): Rewrite check_for_results to use better way of joining process pool (#4703) by @untitaker

_Plus 78 more_

## 23.8.0

### Various fixes & improvements

- fix(Cache): Cache fail open on ValueError (#4615) by @rahul-kumar-saini
- feat(replays): Add low cardinality encoding to select columns (#4601) by @cmanallen
- feat: Group the validation warnings for each topic separately (#4570) by @lynnagara
- fix(oncall): Query execution dataset metric tag (#4608) by @rahul-kumar-saini
- feat(replays): Add materialized counts for array columns (#4603) by @cmanallen
- search_issues -> group_attributes join relationship (#4588) by @barkbarkimashark
- feat(async-queries): Add ability to override query settings for async queries (#4584) by @davidtsuk
- ref: bump sentry-kafka-schemas to 0.1.25 (#4610) by @getsentry-bot
- fix(ci): improve caching (#4521) by @dbanda
- feat(replay): pass start_time as received header (#4548) by @bmckerry
- fix(capman): rate limit overrides should not collide (#4604) by @volokluev
- feat(db_query): Randomized Query IDs (#4605) by @rahul-kumar-saini
- Remove the test regions from rollbacks (#4599) by @mattgauntseo-sentry
- feat(capman): implement rate limit overrides (#4563) by @volokluev
- pass through subscriptions for new concurrent rate limiter (#4587) by @volokluev
- ci: only render gocd pipelines if relevant files are changed (#4591) by @joshuarli
- bump readiness (#4590) by @barkbarkimashark
- meta: Bump new development version (e7d99451)

## 23.7.2

### Various fixes & improvements

- feat(group_attributes): add ability to join events -> group_attributes  (#4586) by @barkbarkimashark
- cleanup(capman): remove project throttler (#4576) by @volokluev
- feat(consumer): add health check step to consumers (#4508) by @dbanda
- fix(capman): Bake in `is_enforced` behavior into every allocation policy (#4585) by @volokluev
- Update gocd-jsonnet to v1.4.1 (#4579) by @mattgauntseo-sentry
- Revert "feat(db_query): Readthrough Cache entire `db_query()` pipeline (#4506)" (946c90a5) by @getsentry-bot
- ref: bump sentry-arroyo to 2.14.2 (#4569) by @getsentry-bot
- turn on rate limit policy for transactions (#4572) by @volokluev
- feat(db_query): Readthrough Cache entire `db_query()` pipeline (#4506) by @rahul-kumar-saini
- ref(admin): Upload sourcemaps to Sentry for the admin tool (#4441) by @evanh
- feat(oncall): break down query execution metrics by table and referrer (#4573) by @volokluev
- feat(prod-queries): Add button to view all allowed projects (#4574) by @davidtsuk
- feat(consumer): Add transactions consumer SLO (#4442) by @ayirr7
- feat(schema): add replay_id to table entities and storages (#4565) by @JoshFerge
- ref: bump sentry-kafka-schemas to 0.1.23 (#4571) by @getsentry-bot
- feat(rust-consumer): Port querylog processor to Rust (#4562) by @lynnagara
- feat(group-attributes): Change the readiness state for migration to partial (#4567) by @lynnagara
- Revert back to snuba group for role access (#4550) by @mattgauntseo-sentry
- feat(capman): ratelimit allocation policy (no overrides) (#4536) by @volokluev
- Switch is ST check to library version (#4568) by @mattgauntseo-sentry
- feat(clickhouse): Enable 23.3 (#4566) by @lynnagara
- Add customer 1 and 2 to pipedream (#4564) by @mattgauntseo-sentry
- meta: Bump new development version (24a00034)

## 23.7.1

### Various fixes & improvements

- ref: bump sentry-kafka-schemas to 0.1.22 (#4559) by @getsentry-bot
- ref: Remove manual collection of librdkafka stats (#4542) by @lynnagara
- feat: Banish fetching untyped runtime config (#4540) by @lynnagara
- Fix validation pipeline (#4547) by @mattgauntseo-sentry
- feat(rust-consumer): Add RunTaskInThreads strategy (#4537) by @lynnagara
- ref: Remove unnecessary config (#4541) by @lynnagara
- fix(prod-queries): Remove project validation in debug env (#4549) by @davidtsuk
- Update gocd jsonnet libs and fix rollbacks (#4544) by @mattgauntseo-sentry
- fix(cardinality-analyzer): Fix CSV escaping (#4546) by @gggritso
- build(deps): bump flask from 2.2.2 to 2.2.5 (#4543) by @dependabot
- build(deps): bump werkzeug from 2.2.2 to 2.2.3 (#3771) by @dependabot
- chore: bump sentry-kafka-schemas to 0.1.21 (#4529) by @ayirr7
- ref(snubsplain): Add steps for snql parsing and query mappers (#4486) by @evanh
- feat(group-attributes): expose storage and entity, process messages (#4507) by @barkbarkimashark
- ref: Simplify querylog processor more (#4520) by @lynnagara
- ci: add GoCD pipeline validation (#4528) by @joshuarli
- ref(capman): split the rate limit function into a start and finish phase (#4532) by @volokluev
- ref: Simplify querylog processor (#4511) by @lynnagara
- Add elastic agent to rollback pipeline (#4534) by @mattgauntseo-sentry
- set min_retention_days to retention_days (#4533) by @john-z-yang
- feat(admin): Add new trace logs view (#4510) by @enochtangg
- Revert "Revert "feat(replays): Add click_is_dead and click_is_rage columns (#4470)"" (#4517) by @cmanallen
- Enable Sentry `Hub` propagation (#4530) by @gggritso
- Fix canaries and migrations in s4s snuba (#4531) by @mattgauntseo-sentry

_Plus 1 more_

## 23.7.0

### Various fixes & improvements

- Switch to pipedream pipelines (#4526) by @mattgauntseo-sentry
- feat(rust-consumer): Support parallel insert to ClickHouse (#4519) by @lynnagara
- run search_issues migration in self-hosted (#4518) by @hubertdeng123
- bump materialization_version to 2 (#4505) by @john-z-yang
- perf: Allow parallel clickhouse insert (#4516) by @lynnagara
- add facilities to insert aggregation_options in processor (#4509) by @john-z-yang
- feat(subscriptions): add clickhouse connection check (#4512) by @dbanda
- Revert "feat(replays): Add click_is_dead and click_is_rage columns (#4470)" (0788e2ad) by @getsentry-bot
- ref: bump sentry-kafka-schemas to 0.1.19 (#4515) by @getsentry-bot
- migration(group_attributes): create new GroupAttributes table that mirrors a subset of columns from sentry (#4496) by @barkbarkimashark
- Allow snuba/clusters/storage_sets.py  in migrations check (#4514) by @dbanda
- feat(replays): Add click_is_dead and click_is_rage columns (#4470) by @cmanallen
- add migrations for new distributions matview (#4504) by @john-z-yang
- fix: Actually resubmit carried over message in clickhouse strategy (#4502) by @lynnagara
- feat(replays): Add migration for click_is_dead and click_is_rage columns (#4469) by @cmanallen
- ref: bump sentry-kafka-schemas to 0.1.18 (#4501) by @getsentry-bot
- fix(rust-consumer): Actually commit messages even if we are skipping the clickhouse write (#4500) by @lynnagara
- feat(generic-metrics): Add aggregation options for generic metrics raw tables (#4467) by @john-z-yang
- ref(rust-consumer): Move clickhouse_client out of rust_arroyo (#4499) by @lynnagara
- feat(rust-consumer): Write to ClickHouse (#4490) by @lynnagara
- Fix MultistorageCollector as #4297 (#4475) by @qbx2
- test: Rust CI improvements (#4258) by @lynnagara
- check connections on consumer startup (#4497) by @dbanda
- Update jsonnet lib and add rollback pipeline (#4495) by @mattgauntseo-sentry

_Plus 17 more_

## 23.6.2

### Various fixes & improvements

- add role to migrations stage (#4424) by @dbanda
- experiment(capman,starfish): add ability to turn off concurrent throttling (#4464) by @volokluev
- feat(dlq): If DLQ replay is kicked off, add it to audit log (#4439) by @lynnagara
- Add script for customer deployments (#4438) by @mattgauntseo-sentry
- feat(oncall): Make readthrough cache fail open on redis errors (#4449) by @volokluev
- ref(admin) Allow colons in system queries so that datetimes are valid (#4462) by @evanh
- perm(cardinality-analyzer): Add ingest team (#4458) by @jjbayer
- use mantine text area instead of rich text editor (#4461) by @enochtangg
- perm(prod-queries): Add production queries tool to product tools (#4460) by @davidtsuk
- fix(spans): Add `group_raw` to HexIntColumnProcessor (#4459) by @shruthilayaj
- feat(admin): Add syntax highlighting to snuba admin (#4430) by @enochtangg
- feat(admin): Stop blocking text/images on Snuba Admin replays (#4443) by @rahul-kumar-saini
- ref: Clean up print statements (#4447) by @lynnagara
- fix: Revert debounced commits (#4446) by @untitaker
- test(dlq): Add end to end DLQ consumer test (#4367) by @lynnagara
- ref: bump sentry-redis-tools to 0.1.7 (#4445) by @getsentry-bot
- ref: bump sentry-kafka-schemas to 0.1.17 (#4444) by @getsentry-bot
- feat(prod-queries): Implement backend for prod queries (#4398) by @davidtsuk
- ref: Decouple processing batches and clickhouse batches (#4251) by @untitaker
- ref: bump sentry-kafka-schemas to 0.1.16 (#4436) by @getsentry-bot
- build(deps): bump sentry-relay from 0.8.21 to 0.8.27 (#4427) by @dependabot
- perm(cardinality-analyzer) Update cardinality analyzer member list (#4435) by @davidtsuk
- feat(snubsplain): Use request context to capture query processors (#4381) by @evanh
- ref(admin): Set the user email in Sentry (#4431) by @evanh

_Plus 65 more_

## 23.6.1

### Various fixes & improvements

- fix: Accidentally checked in useless text (#4355) by @lynnagara
- fix(admin): Pin yarn and node using volta (#4349) by @untitaker
- meta: Bump new development version (c2c6e3c3)
- feat(dlq): Redesign admin UI, add API endpoints for replaying (#4347) by @lynnagara

## 23.6.0

### Various fixes & improvements

- ref(DRS): Deprecate dataset experimental flag (#4321) by @enochtangg
- Revert "fix: Temporarily revert dlq topic changes" (#4339) by @lynnagara
- fix(iam-policy): Don't build google cloud API client in debug and test environments (#4344) by @davidtsuk
- feat(prod-queries): Add preliminary UI for the production queries tool (#4345) by @davidtsuk
- ref(iam-policy): Replace user emails with groups in iam_policy (#4346) by @davidtsuk
- feat(spans): Add processors to extract millisecond granularity data (#4313) by @nikhars
- ref(gen_met): Add mapping optimizer to generic metrics (#4336) by @evanh
- Add optional Redis SSL support (#4343) by @frank-m
- feat(spans): Add migration to create start_ms and end_ms columns (#4308) by @nikhars
- ref(admin): Give team-sns google group full access to snuba admin (#4337) by @davidtsuk
- feat(CapMan): Multiple Allocation Policies on a Storage [backend] (#4294) by @rahul-kumar-saini
- chore(spans): Allow spans ingestion based on project sampling (#4214) by @nikhars
- fix: Temporarily revert dlq topic changes (#4338) by @lynnagara
- fix(tests): Skip flaky optimize tests (#4315) by @nikhars
- ref(dlq): Simplify the way DLQ topic is configured (#4333) by @lynnagara
- fix: All consumers have unique DLQ topics (#4335) by @lynnagara
- feat(dlq): DLQ topics API for admin (#4334) by @lynnagara
- Use Google Groups API for authorization (#4306) by @davidtsuk
- fix(dlq): Pass for to configure_metrics (#4329) by @lynnagara
- ref: Upgrade arroyo, and remove now-duplicated rdkafka.total_queue_size metric (#4330) by @untitaker
- fix(gen-metrics): Add tags hash column to the generic metrics dist ta… (#4324) by @nikhars
- ref: Set compression in `build_kafka_producer_configuration()` function (#4220) by @lynnagara
- fix(dlq): ExitAfterNMessages strategy should be first step not last (#4318) by @lynnagara
- feat(spans): Allow spans migrations to run locally (#4323) by @enochtangg

_Plus 40 more_

## 23.5.2

### Various fixes & improvements

- feat(CapMan): Spans Allocation Policy (#4261) by @rahul-kumar-saini
- feat(CapMan): Allocation Policy UI tests (#4200) by @rahul-kumar-saini
- fix(CapMan): organzation_id -> organization_id (#4267) by @rahul-kumar-saini
- fix(DC): Outcomes Raw Entity was under wrong Dataset (#4263) by @rahul-kumar-saini
- reject search issues bad column (#4260) by @volokluev
- Final bypass for subscriptions (#4253) by @fpacifici
- feat(search-issues): expose and process profile_id and replay_id columns (#4164) by @barkbarkimashark
- fix: Fix flaky optimize test (#4252) by @untitaker
- fix(spans): use system instead of platform (#4244) by @dbanda
- promote profiles and functions to complete (#4238) by @enochtangg
- ref: bump sentry-kafka-schemas to 0.1.10 (#4250) by @getsentry-bot
- feat(CapMan): Most throttled Orgs visibility (#4248) by @rahul-kumar-saini
- feat(DRS): Enable readiness state for storages (#4240) by @enochtangg
- ref(dashboard): Add a setting that restricts the tools visible in the dashboard (#4174) by @evanh
- fix: Add querylog `max.message.bytes` to topic configuration (#4221) by @lynnagara
- fix(logs) : use structlog in migrations (#4236) by @dbanda
- feat(dlq): Consumer builder can build a DLQ strategy (#4235) by @lynnagara
- ref: bump sentry-arroyo to 2.11.4 (#4243) by @getsentry-bot
- ref: bump sentry-arroyo to 2.11.3 (#4241) by @getsentry-bot
- fix(parsing): Update error message for missing datetime column comparison condition (#4189) by @davidtsuk
- fix: Querylog producer (#4219) by @lynnagara
- fix: Fix attribution producer (#4218) by @lynnagara
- test(dlq): Add a small test for ExitAfterNMessages strategy (#4234) by @lynnagara
- feat(dlq): Add is_valid() method to DlqInstruction class (#4233) by @lynnagara

_Plus 12 more_

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
