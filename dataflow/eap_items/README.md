# eap-items-dataflow

A streaming Dataflow pipeline (Python Apache Beam, packaged as a Flex Template). It reads
`sentry_protos.snuba.v1.TraceItem` protobufs from the Kafka topic `snuba-items` on the
s4s2 **spans** cluster and writes them to the BigQuery table `sentry-s4s2.eap.items`.

The target table, its dataset, the worker service account and the staging bucket are defined
in the ops repo under `terragrunt/regions/multi-tenant/bigquery/eap/` (schema:
`items_schema.hcl`). The pipeline never creates or alters the table
(`CREATE_NEVER` / `WRITE_APPEND`).

```
Kafka snuba-items (32 partitions, TraceItem bytes)
  └─ ReadFromKafka            (Java, cross-language, Runner v2)
     └─ TraceItemToRow        (Python: decode → sample by trace → row dict)
        ├─ rows ─ WriteToBigQuery (Storage Write API, at-least-once by default)
        │           └─ failed rows ─┐
        └─ dead_letter ─────────────┴─ log + counter [+ JSONL to GCS]
```

## Layout

| Path | Purpose |
| --- | --- |
| `src/eap_items_dataflow/transform.py` | Pure `TraceItem` → row conversion and sampling (no Beam) |
| `src/eap_items_dataflow/pipeline.py` | Pipeline options, Kafka read, DoFn, BigQuery write, dead-letter handling |
| `main.py` | Entry point for the Flex Template and local runs |
| `Dockerfile` | Flex Template launcher **and** SDK worker image (same image) |
| `metadata.json` | Flex Template parameter metadata |
| `Makefile` | `venv`, `test`, `image`, `push`, `template`, `run`, `cancel` |
| `tests/` | pytest: transform unit tests plus DirectRunner tests of the transform, sampling and dead-letter steps |

## Versions

- `apache-beam[gcp]==2.75.0`, `sentry-protos==0.76.0`, Python 3.11.
- Beam 2.76.0 does **not** work here. It depends on `google-cloud-bigtable>=2.42`, which needs
  `protobuf>=6`, but `sentry-protos` pins `protobuf<6`. Before bumping either package, check that
  `uv pip install` still resolves.

## Row semantics

Snuba's EAP consumer (`rust_snuba/src/processors/eap_items.rs`, commit
`5b82ab9c1`) is the reference. The line numbers below refer to that commit.

| Column | Value | Snuba reference |
| --- | --- | --- |
| `organization_id`, `project_id` | `uint64` as a decimal string | `eap_items.rs:383-384` |
| `item_type` | Proto enum name, e.g. `TRACE_ITEM_TYPE_SPAN`. Values unknown to the pinned sentry-protos become `TRACE_ITEM_TYPE_<n>` instead of being dropped | Snuba stores the `u8` (`:385`) |
| `timestamp` | `TraceItem.timestamp`, microsecond precision. A missing timestamp is dead-lettered | Snuba: `timestamp.seconds as u32` (`:362`, `:390`) |
| `trace_id` | Parsed as a UUID (dashes or no dashes, any case) and written as 32 lowercase hex chars without dashes. An invalid value is dead-lettered | `Uuid::parse_str` (`:386`). The read path returns it without dashes via `UUIDColumnProcessor` |
| `item_id` | See [item_id](#item_id) | `read_item_id` (`:475-482`) |
| `attributes` | JSON object, see [attributes](#attributes) | `:402-413`, `:119-129` |
| `retention_days` | See [retention_days](#retention_days) | `enforce_retentions` (`:101-108`), `utils.rs:50-79` |
| `sample_rate` | See [sample_rate](#sample_rate) | `:415-438` |

### item_id

`TraceItem.item_id` holds the id as **little-endian** bytes. Producers build it as
`int(hex, 16).to_bytes(16, "little")` (sentry `src/sentry/utils/eap.py` `hex_to_item_id`) or
`uuid.as_u128().to_le_bytes()` (relay `relay-server/src/processing/utils/store.rs:216`).

- **Ingest:** Snuba reads the **first 16 bytes** as a little-endian `u128` into a `UInt128`
  column. Fewer than 16 bytes is an error (`eap_items.rs:475-482`). This pipeline does the
  same, and sends short ids to the dead-letter output.
- **Read path:** Snuba renders the column with `HexIntColumnProcessor(size=32)`
  (`snuba/datasets/configuration/events_analytics_platform/storages/eap_items.yaml:127-130`,
  `snuba/query/processors/physical/hexint_column_processor.py:34-54`). It produces
  `lower(leftPad(hex(item_id), length > 16 ? 32 : 16, '0'))`.
- **Here:** `format(int.from_bytes(b[:16], "little"), "016x" if < 2**64 else "032x")`, which
  is identical to what Snuba returns for `sentry.item_id`:
  - Span ids come back as 16 hex chars, e.g. `a1b2c3d4e5f60718`.
  - UUID-based ids (logs, errors, metrics, …) come back as 32 hex chars, equal to `uuid.hex`.

  This is **not** a dashed UUID. A dashed UUID would not match Sentry's span ids or Snuba API
  output.

### sample_rate

Two sampling layers are combined into one value:

1. **Upstream (Snuba semantics):** `snuba_factor` is computed exactly as `sampling_factor` in
   `eap_items.rs:415-438`:
   - Multiply in `client_sample_rate` and `server_sample_rate`, but only if each is in
     `(0, 1]`. The proto default `0.0` means "unset" and counts as `1.0`.
   - Round to 1e-6. Like Rust's `f64::round`, halves round away from zero.
   - Clamp to a minimum of 1e-6.
2. **Pipeline:** `--sample_rate` in `(0, 1]` (default `1.0`).
   - An item is kept iff `blake2b_64(trace_id_bytes, key=--sample_seed) / 2^64 < sample_rate`.
   - The decision uses only the normalized trace id, so it is deterministic and keeps or drops
     **whole traces** (every span, log, error and metric of a trace) together. It is also stable
     across restarts, replays and workers.
   - Changing `--sample_seed` selects an independent subset.

**Stored value:** `sample_rate = snuba_factor × pipeline_sample_rate`. This is the probability
that the original event is represented by this row, so `SUM(1 / sample_rate)` estimates the
original count, the same way Snuba uses `sampling_weight = round(1 / sampling_factor)`.

The individual `client_sample_rate` and `server_sample_rate` are not stored; see follow-ups.

### retention_days

This follows Snuba's `enforce_standard_retention`:

- `0` (unset) becomes `--retention_default_days` (default 30).
- Anything else is capped at `--retention_max_days` (default 90).

This matters for BigQuery: in `eap.items`, `NULL` or `<= 0` means *keep forever* (see the ops
README), so passing a raw `0` through would keep those rows indefinitely.

Snuba can override 30/90 at runtime through sentry-options (`retention_days.standard.*`). If that
override is used in s4s2, set the two pipeline options to match.

### attributes

The JSON object is built from `TraceItem.attributes`, where each `AnyValue` is encoded as plain
JSON:

| AnyValue | JSON |
| --- | --- |
| `string_value` | string |
| `bool_value` | bool |
| `int_value` | integer (full int64 precision; use `LAX_INT64` / `INT64()` in BigQuery) |
| `double_value` | number. `NaN`, `Infinity` and `-Infinity` become strings, because JSON and BigQuery reject them |
| `array_value` | array (recursive, mixed types kept in order) |
| `kvlist_value` | object (recursive) |
| `bytes_value` | base64 string |
| unset | `null` |

Differences from Snuba:

- Snuba drops `bytes` and `kvlist` values and nested arrays (`eap_items.rs:409-410`, `:594-596`).
  This pipeline keeps them.
- Snuba also double-writes ints as floats (`:558-562`). JSON doesn't need that.
- The value type is not stored separately. `1` (int) and `1.0` (double) serialize differently
  (`1` vs `1.0`), so `JSON_TYPE` / `LAX_*` can still tell them apart in most cases. See
  follow-ups for the case where they can't.

Like Snuba (`:124-129`), `TraceItem.received` is added as the int attribute
`sentry._internal.received_at` (epoch seconds). Snuba's `sentry._internal.ingested_at` (the
consumer's wall clock) is **not** added, because it would make rows non-deterministic across
replays.

### Dead letters

Messages that fail to decode or convert go to a `dead_letter` side output instead of crashing
the job, as do rows BigQuery rejects. Failures include:

- empty payloads
- protobuf decode errors
- invalid `trace_id`
- `item_id` shorter than 16 bytes
- missing timestamp
- any unexpected exception

Each dead letter is:

- logged at WARNING level
- counted in the `eap_items/dead_letter_invalid`, `dead_letter_unexpected_error` and
  `dead_letter_bigquery` counters (visible in the Dataflow UI and Cloud Monitoring)
- written as JSONL (`{stage, error, payload_b64, payload_size, observed_at}`) to
  `--dead_letter_path` if set, one file per `--dead_letter_window_seconds` window

Kafka records use the **processing-time** timestamp policy. CreateTime was deliberately not used: KafkaIO's cross-language CreateTime policy allows zero delay, so out-of-order producer timestamps within a partition would become late data and be dropped by the windowed dead-letter file writer. Nothing else in the pipeline depends on element timestamps.

Other counters: `items_processed`, `rows_emitted`, `rows_sampled_out`, and the
`payload_bytes` distribution.

## Pipeline options

| Option | Default | Notes |
| --- | --- | --- |
| `--bootstrap_servers` | (required) | Comma-separated `host:port` list |
| `--topic` | `snuba-items` | |
| `--consumer_group_id` | `eap-bigquery-items` | Offsets are committed by KafkaIO when checkpoints finalize (`commit_offset_in_finalize`); Kafka auto-commit is off |
| `--output_table` | `sentry-s4s2:eap.items` | |
| `--sample_rate` | `1.0` | Pipeline trace sampling, see above |
| `--sample_seed` | `""` | |
| `--start_offset` | `latest` | Sets `auto.offset.reset`. Only applies when the group has no committed offset |
| `--start_read_time_ms` | `-1` | If `>= 0`, ignores committed offsets and starts every partition at this broker timestamp |
| `--write_method` | `STORAGE_WRITE_API` | `STORAGE_WRITE_API` (at-least-once, lowest cost and latency), `STORAGE_WRITE_API_EXACTLY_ONCE` (uses `--triggering_frequency_seconds`, auto-sharding), or `STREAMING_INSERTS` |
| `--triggering_frequency_seconds` | `5` | Exactly-once only |
| `--retention_default_days` / `--retention_max_days` | `30` / `90` | |
| `--dead_letter_path` | `""` | e.g. `gs://sentry-dataflow-eap-s4s2/dead-letter/eap-items-bigquery/` |
| `--dead_letter_window_seconds` | `300` | |
| `--kafka_consumer_config` | `{}` | JSON object of extra consumer properties, e.g. `{"max.poll.records":"2000"}` |
| `--max_num_records` | `0` | Testing only. Makes the Kafka read bounded and runs the job in batch mode |

At-least-once delivery (Kafka replay after a failure, or the Storage Write API default stream)
can produce duplicate rows. Deduplicate on `(organization_id, project_id, item_type, item_id)`
at query time if needed.

## Local development

```bash
make venv            # uv venv --python 3.11 .venv && uv pip install -e '.[test]'
make test            # pytest: transform unit tests + DirectRunner tests
```

A local end-to-end run against Kafka needs Java 11+ on `PATH` (for the KafkaIO and BigQuery
expansion services), network access to the brokers, and BigQuery credentials:

```bash
.venv/bin/python main.py --runner=DirectRunner \
  --bootstrap_servers=localhost:9092 --max_num_records=100 \
  --output_table=my-project:scratch.items --write_method=STREAMING_INSERTS \
  --temp_location=gs://my-bucket/tmp
```

## Infra prerequisites (s4s2)

| Status | Item |
| --- | --- |
| Exists (ops `ffa1950691`, PR #22958) | Worker SA `dataflow-eap@sentry-s4s2.iam.gserviceaccount.com`, bucket `gs://sentry-dataflow-eap-s4s2`, dataset/table `sentry-s4s2.eap.items`, deployers `team-events-analytics-platform@sentry.io` (`roles/dataflow.developer` + `actAs`) |
| Committed locally in ops (`ebb6abf9fb`), **not applied** | Firewall `allow-dataflow-to-kafka` (tcp:9092, tag `dataflow` → `kafka`) and `allow-dataflow-to-dataflow` (tcp:12345-12346, `dataflow` ↔ `dataflow`) on `sentry-default`, in `terragrunt/regions/multi-tenant/network/s4s2/local.hcl` |
| **Missing** | Artifact Registry Docker repo in `sentry-s4s2` / `us-east1` (the Makefile assumes `dataflow`) |
| **Missing** | `roles/artifactregistry.reader` on that repo for `dataflow-eap@…`, plus `roles/artifactregistry.writer` for whoever or whatever pushes the image |
| To verify | The SA's roles: `roles/dataflow.worker`, `roles/storage.objectAdmin` on the bucket, and `roles/bigquery.dataEditor` on `eap` (the ops README says it has `dataEditor` and `jobUser`) |
| To verify | Private Google Access on subnet `sentry-default`. It is needed because of `--disable-public-ips` (`secrets.hcl` sets `subnet_private_access = true` for the default subnet). |
| Network note | With no public IPs there is no PyPI or Maven access at runtime. The image therefore bakes in all Python deps, a JRE and both Beam expansion-service jars (`beam-sdks-java-io-expansion-service`, `beam-sdks-java-io-google-cloud-platform-expansion-service`). The worker-side Java harness image for the cross-language transforms (`apache/beam_java<N>_sdk`, where N is the launcher JRE's major version: 17 with Debian bookworm's `default-jre-headless`) is rewritten by the Dataflow runner to `gcr.io/cloud-dataflow/v1beta3/...` and pulled through Private Google Access |

## Build and deploy (s4s2)

This assumes valid `gcloud auth login` / `gcloud auth application-default login` and that the
Artifact Registry repo exists. The equivalent Makefile targets are `image`, `push`, `template`
and `run`.

```bash
PROJECT=sentry-s4s2
REGION=us-east1
TAG=$(git rev-parse --short HEAD)
IMAGE=${REGION}-docker.pkg.dev/${PROJECT}/dataflow/eap-items-dataflow:${TAG}
BUCKET=gs://sentry-dataflow-eap-s4s2
TEMPLATE=${BUCKET}/templates/eap-items-dataflow-${TAG}.json
BOOTSTRAP=kafka-spans-0.us-east1-b.c.sentry-s4s2.internal.:9092,kafka-spans-1.us-east1-c.c.sentry-s4s2.internal.:9092,kafka-spans-2.us-east1-d.c.sentry-s4s2.internal.:9092

# 1. Build and push the image (launcher + worker SDK container)
gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker build --platform linux/amd64 -t ${IMAGE} .
docker push ${IMAGE}

# 2. Build the Flex Template spec
gcloud dataflow flex-template build ${TEMPLATE} \
  --project=${PROJECT} \
  --image=${IMAGE} \
  --sdk-language=PYTHON \
  --metadata-file=metadata.json

# 3. Launch the streaming job (Runner v2, Streaming Engine, private IPs only)
gcloud dataflow flex-template run eap-items-bigquery \
  --project=${PROJECT} \
  --region=${REGION} \
  --template-file-gcs-location=${TEMPLATE} \
  --service-account-email=dataflow-eap@${PROJECT}.iam.gserviceaccount.com \
  --subnetwork=regions/${REGION}/subnetworks/sentry-default \
  --disable-public-ips \
  --staging-location=${BUCKET}/staging \
  --temp-location=${BUCKET}/temp \
  --worker-machine-type=n2-standard-4 \
  --max-workers=4 \
  --enable-streaming-engine \
  --additional-experiments=use_runner_v2 \
  --parameters="^~^sdk_container_image=${IMAGE}~sdk_location=container~bootstrap_servers=${BOOTSTRAP}~sample_rate=1.0~dead_letter_path=${BUCKET}/dead-letter/eap-items-bigquery/"
```

Notes on the launch command:

- The `^~^` prefix switches the `--parameters` separator to `~`, because `bootstrap_servers`
  contains commas.
- `sdk_location=container` makes workers use the Beam SDK already installed in the image, so
  nothing is downloaded from PyPI.
- Dataflow workers get the `dataflow` network tag automatically, which is what the firewall
  rules target.
- `--additional-experiments=use_runner_v2` is explicit even though Python streaming jobs
  already use Runner v2. Cross-language KafkaIO needs it.

Operations:

```bash
# Update in place, e.g. a new image or a new sample_rate (keeps the consumer group and state)
gcloud dataflow flex-template run eap-items-bigquery --update ...same flags...

# Drain (flushes in-flight data, then stops)
gcloud dataflow jobs list --project=${PROJECT} --region=${REGION} --status=active --filter=name=eap-items-bigquery
gcloud dataflow jobs drain JOB_ID --project=${PROJECT} --region=${REGION}
```

## Follow-ups: proto fields not in the BigQuery schema

These `TraceItem` fields are currently **not** stored. Adding any of them would require an ops
schema change, which is out of scope here.

| Proto field | Snuba column | Notes |
| --- | --- | --- |
| `client_sample_rate`, `server_sample_rate` | same names | Only their product (× pipeline rate) is stored, in `sample_rate` |
| (derived) `sampling_weight` | `sampling_weight` | Can be computed as `ROUND(1 / sample_rate)` |
| `received` (Timestamp) | used for metrics and `sentry._internal.received_at` | Only seconds are stored, inside `attributes` |
| `downsampled_retention_days` | `downsampled_retention_days` (`enforce_retentions`: at least `retention_days`, default 396) | BigQuery has no downsampled tiers. Decide whether this should drive `retention_days` instead |
| `conversation_id` | `ai_conversation_id` | |
| `session_id` | `session_id` (UUID; a random UUID is generated if missing or invalid) | |
| `outcomes` (`category_count[]`, `key_id`) | not stored by Snuba either (used for accepted outcomes) | |
| (derived) `indexed_name` | `sentry.op` for spans, `sentry.metric_name` for metrics | Still present in `attributes` |
| Kafka broker timestamp | `received_at` (when `eap_items_emit_received_at` is set) | |
| Attribute value types | Snuba's typed maps | `attributes` JSON loses the int vs double distinction for whole-number doubles (`1.0` is encoded as `1.0`, but BigQuery may normalize it). Consider a separate type map if exact typing matters |

Other open items:

- Snuba optionally dead-letters items whose timestamp falls outside the current weekly
  partition (`eap_items_dlq_grace_period_min`). That is a ClickHouse-specific concern, so it
  is not implemented here.
