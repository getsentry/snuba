use once_cell::sync::Lazy;

pub static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

pub fn processor_for_schema(schema: &str) -> &str {
    match schema {
        "snuba-spans" => "SpansMessageProcessor",
        "snuba-queries" => "QuerylogProcessor",
        "processed-profiles" => "ProfilesMessageProcessor",
        "profiles-call-tree" => "FunctionsMessageProcessor",
        "ingest-replay-events" => "ReplaysProcessor",
        "snuba-generic-metrics" => "MetricsSummariesMessageProcessor",
        "outcomes" => "OutcomesProcessor",
        _ => todo!("need to add new schemas and processors"),
    }
}

pub fn payloads_for_schema(schema: &str) -> &[&str] {
    match schema {
        "snuba-spans" => SPANS,
        "snuba-queries" => QUERIES,
        "processed-profiles" => PROFILES,
        "profiles-call-tree" => FUNCTIONS,
        "ingest-replay-events" => REPLAYS,
        "snuba-generic-metrics" => METRICS,
        "outcomes" => OUTCOMES,
        _ => todo!("need to define more payloads"),
    }
}

const SPANS: &[&str] = &[r#"
{
  "event_id": "dcc403b73ef548648188bbfa6012e9dc",
  "organization_id": 69,
  "project_id": 1,
  "trace_id": "deadbeefdeadbeefdeadbeefdeadbeef",
  "span_id": "deadbeefdeadbeef",
  "parent_span_id": "deadbeefdeadbeef",
  "segment_id": "deadbeefdeadbeef",
  "group_raw": "b640a0ce465fa2a4",
  "profile_id": "deadbeefdeadbeefdeadbeefdeadbeef",
  "is_segment": false,
  "start_timestamp_ms": 1691105878720,
  "received": 169110587919.123,
  "duration_ms": 1000,
  "exclusive_time_ms": 1000,
  "retention_days": 90,
  "tags": {
    "tag1": "value1",
    "tag2": "123",
    "tag3": "True"
  },
  "sentry_tags": {
    "http.method": "GET",
    "action": "GET",
    "domain": "targetdomain.tld:targetport",
    "module": "http",
    "group": "deadbeefdeadbeef",
    "status": "ok",
    "system": "python",
    "status_code": "200",
    "transaction": "/organizations/:orgId/issues/",
    "transaction.op": "navigation",
    "op": "http.client",
    "transaction.method": "GET"
  },
  "measurements": {
    "http.response_content_length": {
      "value": 100.0,
      "unit": "byte"
    }
  },
  "_metrics_summary": {
    "c:sentry.events.outcomes@none": [
      {
        "count": 1,
        "max": 1.0,
        "min": 1.0,
        "sum": 1.0,
        "tags": {
          "category": "error",
          "environment": "unknown",
          "event_type": "error",
          "outcome": "accepted",
          "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
          "topic": "outcomes-billing",
          "transaction": "sentry.tasks.store.save_event"
        }
      }
    ],
    "c:sentry.events.post_save.normalize.errors@none": [
      {
        "count": 1,
        "max": 0.0,
        "min": 0.0,
        "sum": 0.0,
        "tags": {
          "environment": "unknown",
          "event_type": "error",
          "from_relay": "False",
          "release": "backend@2af74c237fbd61489a1ccc46650f4f85befaf8b8",
          "transaction": "sentry.tasks.store.save_event"
        }
      }
    ]
  }
}
"#];

const QUERIES: &[&str] = &[
    r#"
{
  "request": {
    "id": "db4e2fc48df34d69be0ada66ab2ed2bd",
    "body": {
      "legacy": true,
      "query": "redacted",
      "dataset": "discover",
      "app_id": "legacy",
      "tenant_ids": {
        "legacy": "legacy"
      },
      "parent_api": "/api/0/organizations/{organization_slug}/issues-count/"
    },
    "referrer": "search",
    "team": "<unknown>",
    "feature": "<unknown>",
    "app_id": "legacy"
  },
  "dataset": "discover",
  "entity": "discover_events",
  "start_timestamp": 1676674553,
  "end_timestamp": 1677884153,
  "query_list": [
    {
      "sql": "redacted",
      "sql_anonymized": "redacted",
      "start_timestamp": 1676674553,
      "end_timestamp": 1677884153,
      "stats": {
        "clickhouse_table": "errors_dist_ro",
        "final": false,
        "referrer": "search",
        "sample": 1,
        "totals_mode": "after_having_inclusive",
        "merge_tree_max_rows_to_use_cache": 1048576,
        "max_query_size": 524288,
        "merge_tree_coarse_index_granularity": 8,
        "max_execution_time": 30,
        "max_threads": 10,
        "merge_tree_min_rows_for_concurrent_read": 163840,
        "max_streams_to_max_threads_ratio": 1,
        "query_id": "3c369a12aedfa413840c5449a1573575",
        "triggered_rate_limiter": "project_referrer"
      },
      "status": "rate-limited",
      "trace_id": "095488ec34424e139d3c474992b21cc8",
      "profile": {
        "time_range": 14,
        "table": "errors_dist_ro",
        "all_columns": [
          "errors_dist_ro._tags_hash_map",
          "errors_dist_ro.deleted",
          "errors_dist_ro.group_id",
          "errors_dist_ro.project_id",
          "errors_dist_ro.timestamp"
        ],
        "multi_level_condition": false,
        "where_profile": {
          "columns": [
            "errors_dist_ro._tags_hash_map",
            "errors_dist_ro.deleted",
            "errors_dist_ro.group_id",
            "errors_dist_ro.timestamp"
          ],
          "mapping_cols": []
        },
        "groupby_cols": ["errors_dist_ro.group_id"],
        "array_join_cols": []
      },
      "result_profile": null,
      "request_status": "table-rate-limited",
      "slo": "against"
    }
  ],
  "status": "rate-limited",
  "request_status": "table-rate-limited",
  "slo": "against",
  "timing": {
    "timestamp": 1677884153,
    "duration_ms": 102,
    "marks_ms": {
      "cache_get": 2,
      "cache_set": 2,
      "get_configs": 0,
      "prepare_query": 56,
      "validate_schema": 40
    },
    "tags": {}
  },
  "projects": [123123123],
  "snql_anonymized": "redacted"
}
"#,
    r#"
{
  "request": {
    "id": "24a78d10a0134f2aa6367ba2a393b504",
    "body": {
      "legacy": true,
      "query": "MATCH (events) SELECT count() AS `count`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen` BY tags_key, tags_value WHERE timestamp >= toDateTime('2023-02-08T21:07:12.769001') AND timestamp < toDateTime('2023-02-08T21:12:39.015094') AND project_id IN tuple(1) AND project_id IN tuple(1) AND group_id IN tuple(5) ORDER BY count DESC LIMIT 4 BY tags_key",
      "dataset": "events",
      "app_id": "legacy",
      "parent_api": "/api/0/issues|groups/{issue_id}/tags/"
    },
    "referrer": "tagstore.__get_tag_keys_and_top_values",
    "team": "<unknown>",
    "feature": "<unknown>",
    "app_id": "legacy"
  },
  "dataset": "events",
  "entity": "events",
  "start_timestamp": 1675919232,
  "end_timestamp": 1675919559,
  "query_list": [
    {
      "sql": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), 1) AS _snuba_tags_key), (tupleElement(snuba_all_tags, 2) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(5)) WHERE equals(deleted, 0) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(1)) AND in(_snuba_project_id, tuple(1)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
      "sql_anonymized": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), -1337) AS _snuba_tags_key), (tupleElement(snuba_all_tags, -1337) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(-1337)) WHERE equals(deleted, -1337) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(-1337)) AND in(_snuba_project_id, tuple(-1337)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
      "start_timestamp": 1675919232,
      "end_timestamp": 1675919559,
      "stats": {
        "clickhouse_table": "errors_local",
        "final": false,
        "referrer": "tagstore.__get_tag_keys_and_top_values",
        "sample": null,
        "table_rate": 0.6,
        "table_concurrent": 1,
        "project_rate": 0.6333333333333333,
        "project_concurrent": 1,
        "consistent": false,
        "result_rows": 22,
        "result_cols": 5,
        "query_id": "9079915acbacff0804ed45c72b865024"
      },
      "status": "success",
      "trace_id": "8377f280e7eb4754a7f20df73ce2cf37",
      "profile": {
        "time_range": null,
        "table": "errors_local",
        "all_columns": [
          "errors_local.deleted",
          "errors_local.group_id",
          "errors_local.project_id",
          "errors_local.tags.key",
          "errors_local.tags.value",
          "errors_local.timestamp"
        ],
        "multi_level_condition": false,
        "where_profile": {
          "columns": [
            "errors_local.deleted",
            "errors_local.project_id",
            "errors_local.timestamp"
          ],
          "mapping_cols": []
        },
        "groupby_cols": ["errors_local.tags.key", "errors_local.tags.value"],
        "array_join_cols": ["errors_local.tags.key", "errors_local.tags.value"]
      },
      "result_profile": {
        "bytes": 1305,
        "blocks": 1,
        "rows": 22,
        "elapsed": 0.009863138198852539
      },
      "request_status": "success",
      "slo": "for"
    }
  ],
  "status": "success",
  "request_status": "success",
  "slo": "for",
  "timing": {
    "timestamp": 1675890758,
    "duration_ms": 55,
    "marks_ms": {
      "cache_get": 2,
      "cache_set": 6,
      "execute": 10,
      "get_configs": 0,
      "prepare_query": 15,
      "rate_limit": 5,
      "validate_schema": 15
    },
    "tags": {}
  },
  "projects": [1],
  "snql_anonymized": "MATCH Entity(events) SELECT tags_key, tags_value, (count() AS count), (min(timestamp) AS first_seen), (max(timestamp) AS last_seen) GROUP BY tags_key, tags_value WHERE greaterOrEquals(timestamp, toDateTime('$S')) AND less(timestamp, toDateTime('$S')) AND in(project_id, tuple(-1337)) AND in(project_id, tuple(-1337)) AND in(group_id, tuple(-1337)) ORDER BY count DESC LIMIT 4 BY tags_key LIMIT 1000 OFFSET 0"
}
"#,
    r#"
{
  "request": {
    "id": "24a78d10a0134f2aa6367ba2a393b504",
    "body": {
      "legacy": true,
      "query": "MATCH (events) SELECT count() AS `count`, min(timestamp) AS `first_seen`, max(timestamp) AS `last_seen` BY tags_key, tags_value WHERE timestamp >= toDateTime('2023-02-08T21:07:12.769001') AND timestamp < toDateTime('2023-02-08T21:12:39.015094') AND project_id IN tuple(1) AND project_id IN tuple(1) AND group_id IN tuple(5) ORDER BY count DESC LIMIT 4 BY tags_key",
      "dataset": "events",
      "app_id": "legacy",
      "parent_api": "/api/0/issues|groups/{issue_id}/tags/"
    },
    "referrer": "tagstore.__get_tag_keys_and_top_values",
    "team": "<unknown>",
    "feature": "<unknown>",
    "app_id": "legacy"
  },
  "dataset": "events",
  "entity": "events",
  "start_timestamp": 1675919232,
  "end_timestamp": 1675919559,
  "query_list": [
    {
      "sql": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), 1) AS _snuba_tags_key), (tupleElement(snuba_all_tags, 2) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(5)) WHERE equals(deleted, 0) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(1)) AND in(_snuba_project_id, tuple(1)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
      "sql_anonymized": "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> (x, y)), tags.key, tags.value)) AS snuba_all_tags), -1337) AS _snuba_tags_key), (tupleElement(snuba_all_tags, -1337) AS _snuba_tags_value), (count() AS _snuba_count), (min((timestamp AS _snuba_timestamp)) AS _snuba_first_seen), (max(_snuba_timestamp) AS _snuba_last_seen) FROM errors_local PREWHERE in((group_id AS _snuba_group_id), tuple(-1337)) WHERE equals(deleted, -1337) AND greaterOrEquals(_snuba_timestamp, toDateTime('2023-02-08T21:07:12', 'Universal')) AND less(_snuba_timestamp, toDateTime('2023-02-08T21:12:39', 'Universal')) AND in((project_id AS _snuba_project_id), tuple(-1337)) AND in(_snuba_project_id, tuple(-1337)) GROUP BY _snuba_tags_key, _snuba_tags_value ORDER BY _snuba_count DESC LIMIT 4 BY _snuba_tags_key LIMIT 1000 OFFSET 0",
      "start_timestamp": 1675919232,
      "end_timestamp": 1675919559,
      "stats": {
        "clickhouse_table": "errors_local",
        "final": false,
        "referrer": "tagstore.__get_tag_keys_and_top_values",
        "sample": null,
        "table_rate": 0.6,
        "table_concurrent": 1,
        "project_rate": 0.6333333333333333,
        "project_concurrent": 1,
        "consistent": false,
        "result_rows": 22,
        "result_cols": 5,
        "query_id": "9079915acbacff0804ed45c72b865024"
      },
      "status": "success",
      "trace_id": "8377f280e7eb4754a7f20df73ce2cf37",
      "profile": {
        "time_range": null,
        "table": "errors_local",
        "all_columns": [
          "errors_local.deleted",
          "errors_local.group_id",
          "errors_local.project_id",
          "errors_local.tags.key",
          "errors_local.tags.value",
          "errors_local.timestamp"
        ],
        "multi_level_condition": false,
        "where_profile": {
          "columns": [
            "errors_local.deleted",
            "errors_local.project_id",
            "errors_local.timestamp"
          ],
          "mapping_cols": []
        },
        "groupby_cols": ["errors_local.tags.key", "errors_local.tags.value"],
        "array_join_cols": ["errors_local.tags.key", "errors_local.tags.value"]
      },
      "result_profile": {
        "bytes": 1305,
        "blocks": 1,
        "rows": 22,
        "elapsed": 0.009863138198852539
      },
      "request_status": "success",
      "slo": "for"
    }
  ],
  "status": "success",
  "request_status": "success",
  "slo": "for",
  "timing": {
    "timestamp": 1675890758,
    "duration_ms": 55,
    "marks_ms": {
      "cache_get": 2,
      "cache_set": 6,
      "execute": 10,
      "get_configs": 0,
      "prepare_query": 15,
      "rate_limit": 5,
      "validate_schema": 15
    },
    "tags": {}
  },
  "projects": [1],
  "snql_anonymized": "MATCH Entity(events) SELECT tags_key, tags_value, (count() AS count), (min(timestamp) AS first_seen), (max(timestamp) AS last_seen) GROUP BY tags_key, tags_value WHERE greaterOrEquals(timestamp, toDateTime('$S')) AND less(timestamp, toDateTime('$S')) AND in(project_id, tuple(-1337)) AND in(project_id, tuple(-1337)) AND in(group_id, tuple(-1337)) ORDER BY count DESC LIMIT 4 BY tags_key LIMIT 1000 OFFSET 0",
  "organization": 1
}
"#,
];

const PROFILES: &[&str] = &[r#"
{
  "architecture": "x86_64",
  "device_locale": "",
  "device_manufacturer": "",
  "device_model": "",
  "device_os_name": "Linux",
  "device_os_version": "4.14.281-212.502.amzn2.x86_64",
  "duration_ns": 55659715,
  "environment": "staging-staging",
  "profile_id": "f851b3c2d70d4f119b3a49ce9f630194",
  "organization_id": 1,
  "platform": "python",
  "project_id": 1,
  "received": 1687971966,
  "retention_days": 90,
  "trace_id": "cbf7414c931448e98ed3f9b4e239a58e",
  "transaction_id": "6d15f5585cbd4b92945c605314f760cd",
  "transaction_name": "GET /events",
  "version_code": "",
  "version_name": ""
}
"#];

const FUNCTIONS: &[&str] = &[r#"
{
  "functions": [
    {
      "fingerprint": 10027678537215769000,
      "function": "CursorWrapper._execute",
      "package": "django.db.backends.utils",
      "in_app": false,
      "self_times_ns": [
        9968883, 9973116, 9973263, 19933212, 9968120, 9965574, 29905829,
        129589001, 9967363, 19938577, 39881572, 11600464, 49840810, 19938549,
        29913753, 9965667, 19945134, 10003759, 9966104, 9964477, 9969739,
        20809886, 11830791
      ]
    },
    {
      "fingerprint": 888186900786406300,
      "function": "connect",
      "package": "psycopg2",
      "in_app": false,
      "self_times_ns": [19946707]
    },
    {
      "fingerprint": 3734820929566501400,
      "function": "<module>",
      "package": "__main__",
      "in_app": true,
      "self_times_ns": [10289412]
    }
  ],
  "environment": "production",
  "profile_id": "0f2d3a4efaa94234b0eb22b1ee7e69a5",
  "platform": "python",
  "project_id": 1,
  "received": 1687971966,
  "retention_days": 90,
  "timestamp": 1687971965,
  "transaction_name": "GET /api/1/events",
  "transaction_op": "http.server",
  "transaction_status": "ok",
  "http_method": "GET"
}
"#];

const REPLAYS: &[&str] = &[
    r#"
{
  "type": "replay_event",
  "start_time": 1702848658.558295,
  "replay_id": "834f314caae54030a1b0dc52b202f24a",
  "project_id": 1,
  "retention_days": 90,
  "payload": [
    123, 34, 116, 121, 112, 101, 34, 58, 32, 34, 114, 101, 112, 108, 97, 121,
    95, 101, 118, 101, 110, 116, 34, 44, 32, 34, 114, 101, 112, 108, 97, 121,
    95, 105, 100, 34, 58, 32, 34, 56, 51, 52, 102, 51, 49, 52, 99, 97, 97, 101,
    53, 52, 48, 51, 48, 97, 49, 98, 48, 100, 99, 53, 50, 98, 50, 48, 50, 102,
    50, 52, 97, 34, 44, 32, 34, 116, 105, 109, 101, 115, 116, 97, 109, 112, 34,
    58, 32, 49, 55, 48, 50, 56, 52, 56, 54, 53, 56, 46, 53, 53, 56, 50, 57, 53,
    44, 32, 34, 105, 115, 95, 97, 114, 99, 104, 105, 118, 101, 100, 34, 58, 32,
    116, 114, 117, 101, 125
  ]
}
"#,
    r#"
{
  "type": "replay_event",
  "start_time": 1702848658.558295,
  "replay_id": "834f314caae54030a1b0dc52b202f24a",
  "project_id": 1,
  "retention_days": 90,
  "payload": [
    123, 34, 116, 121, 112, 101, 34, 58, 34, 114, 101, 112, 108, 97, 121, 95,
    97, 99, 116, 105, 111, 110, 115, 34, 44, 34, 114, 101, 112, 108, 97, 121,
    95, 105, 100, 34, 58, 34, 56, 51, 52, 102, 51, 49, 52, 99, 97, 97, 101, 53,
    52, 48, 51, 48, 97, 49, 98, 48, 100, 99, 53, 50, 98, 50, 48, 50, 102, 50,
    52, 97, 34, 44, 34, 99, 108, 105, 99, 107, 115, 34, 58, 91, 123, 34, 97,
    108, 116, 34, 58, 34, 83, 117, 98, 109, 105, 116, 32, 70, 111, 114, 109, 34,
    44, 34, 97, 114, 105, 97, 95, 108, 97, 98, 101, 108, 34, 58, 34, 70, 111,
    114, 109, 32, 115, 117, 98, 109, 105, 115, 115, 105, 111, 110, 32, 98, 117,
    116, 116, 111, 110, 34, 44, 34, 99, 108, 97, 115, 115, 34, 58, 91, 34, 104,
    101, 108, 108, 111, 34, 44, 34, 119, 111, 114, 108, 100, 34, 93, 44, 34, 99,
    111, 109, 112, 111, 110, 101, 110, 116, 95, 110, 97, 109, 101, 34, 58, 34,
    83, 105, 103, 110, 85, 112, 70, 111, 114, 109, 34, 44, 34, 101, 118, 101,
    110, 116, 95, 104, 97, 115, 104, 34, 58, 34, 52, 51, 102, 97, 51, 101, 102,
    98, 100, 53, 57, 53, 52, 49, 54, 51, 97, 102, 54, 99, 50, 55, 57, 102, 51,
    102, 53, 48, 56, 56, 99, 57, 34, 44, 34, 105, 100, 34, 58, 34, 105, 100, 34,
    44, 34, 105, 115, 95, 100, 101, 97, 100, 34, 58, 49, 44, 34, 105, 115, 95,
    114, 97, 103, 101, 34, 58, 48, 44, 34, 110, 111, 100, 101, 95, 105, 100, 34,
    58, 49, 44, 34, 114, 111, 108, 101, 34, 58, 34, 98, 117, 116, 116, 111, 110,
    34, 44, 34, 116, 97, 103, 34, 58, 34, 100, 105, 118, 34, 44, 34, 116, 101,
    115, 116, 105, 100, 34, 58, 34, 120, 45, 98, 117, 116, 116, 111, 110, 45,
    115, 117, 98, 109, 105, 116, 45, 102, 111, 114, 109, 34, 44, 34, 116, 101,
    120, 116, 34, 58, 34, 83, 117, 98, 109, 105, 116, 34, 44, 34, 116, 105, 109,
    101, 115, 116, 97, 109, 112, 34, 58, 49, 55, 48, 50, 56, 52, 56, 54, 53, 56,
    46, 53, 53, 56, 50, 57, 53, 44, 34, 116, 105, 116, 108, 101, 34, 58, 34, 67,
    108, 105, 99, 107, 32, 116, 111, 32, 115, 117, 98, 109, 105, 116, 32, 102,
    111, 114, 109, 34, 125, 93, 125
  ]
}
"#,
    r#"
{
  "type": "replay_event",
  "start_time": 1702848658.558295,
  "replay_id": "834f314caae54030a1b0dc52b202f24a",
  "project_id": 1,
  "retention_days": 90,
  "payload": [
    123, 34, 116, 121, 112, 101, 34, 58, 32, 34, 101, 118, 101, 110, 116, 95,
    108, 105, 110, 107, 34, 44, 32, 34, 114, 101, 112, 108, 97, 121, 95, 105,
    100, 34, 58, 32, 34, 56, 51, 52, 102, 51, 49, 52, 99, 97, 97, 101, 53, 52,
    48, 51, 48, 97, 49, 98, 48, 100, 99, 53, 50, 98, 50, 48, 50, 102, 50, 52,
    97, 34, 44, 32, 34, 101, 118, 101, 110, 116, 95, 104, 97, 115, 104, 34, 58,
    32, 34, 55, 55, 53, 99, 98, 99, 49, 57, 52, 48, 99, 53, 52, 57, 55, 50, 57,
    49, 49, 49, 97, 99, 51, 54, 97, 98, 56, 99, 54, 99, 48, 52, 34, 44, 32, 34,
    116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 58, 32, 49, 55, 48, 50, 56,
    52, 56, 54, 53, 56, 46, 53, 53, 56, 50, 57, 53, 44, 32, 34, 108, 101, 118,
    101, 108, 34, 58, 32, 34, 102, 97, 116, 97, 108, 34, 44, 32, 34, 102, 97,
    116, 97, 108, 95, 105, 100, 34, 58, 32, 34, 102, 57, 100, 50, 98, 53, 55,
    53, 51, 48, 55, 50, 52, 50, 49, 53, 97, 57, 53, 53, 50, 57, 56, 52, 51, 99,
    99, 54, 102, 57, 100, 48, 34, 125
  ]
}
"#,
    r#"
{
  "type": "replay_event",
  "start_time": 1687999999.0,
  "replay_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "project_id": 1,
  "retention_days": 90,
  "payload": [
    123, 34, 101, 118, 101, 110, 116, 95, 105, 100, 34, 58, 32, 34, 97, 97, 97,
    97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97,
    97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 34, 44, 32, 34, 114, 101, 112, 108,
    97, 121, 95, 105, 100, 34, 58, 32, 34, 98, 98, 98, 98, 98, 98, 98, 98, 98,
    98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98, 98,
    98, 98, 98, 98, 34, 44, 32, 34, 114, 101, 112, 108, 97, 121, 95, 116, 121,
    112, 101, 34, 58, 32, 34, 115, 101, 115, 115, 105, 111, 110, 34, 44, 32, 34,
    115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 32, 49, 44, 32, 34,
    116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 58, 32, 49, 54, 56, 55, 57,
    48, 52, 48, 57, 51, 46, 49, 57, 57, 44, 32, 34, 114, 101, 112, 108, 97, 121,
    95, 115, 116, 97, 114, 116, 95, 116, 105, 109, 101, 115, 116, 97, 109, 112,
    34, 58, 32, 49, 54, 56, 55, 57, 48, 52, 48, 56, 55, 46, 48, 56, 52, 44, 32,
    34, 117, 114, 108, 115, 34, 58, 32, 91, 93, 44, 32, 34, 101, 114, 114, 111,
    114, 95, 105, 100, 115, 34, 58, 32, 91, 93, 44, 32, 34, 116, 114, 97, 99,
    101, 95, 105, 100, 115, 34, 58, 32, 91, 93, 44, 32, 34, 99, 111, 110, 116,
    101, 120, 116, 115, 34, 58, 32, 123, 34, 98, 114, 111, 119, 115, 101, 114,
    34, 58, 32, 123, 34, 110, 97, 109, 101, 34, 58, 32, 34, 70, 105, 114, 101,
    102, 111, 120, 34, 44, 32, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58,
    32, 34, 49, 49, 50, 46, 48, 34, 44, 32, 34, 116, 121, 112, 101, 34, 58, 32,
    34, 98, 114, 111, 119, 115, 101, 114, 34, 125, 44, 32, 34, 111, 115, 34, 58,
    32, 123, 34, 110, 97, 109, 101, 34, 58, 32, 34, 87, 105, 110, 100, 111, 119,
    115, 34, 44, 32, 34, 118, 101, 114, 115, 105, 111, 110, 34, 58, 32, 34, 62,
    61, 49, 48, 34, 44, 32, 34, 116, 121, 112, 101, 34, 58, 32, 34, 111, 115,
    34, 125, 125, 44, 32, 34, 112, 108, 97, 116, 102, 111, 114, 109, 34, 58, 32,
    34, 106, 97, 118, 97, 115, 99, 114, 105, 112, 116, 34, 44, 32, 34, 114, 101,
    108, 101, 97, 115, 101, 34, 58, 32, 34, 54, 54, 54, 54, 54, 54, 54, 54, 54,
    54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54,
    54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 54, 34, 44, 32, 34, 101, 110,
    118, 105, 114, 111, 110, 109, 101, 110, 116, 34, 58, 32, 34, 112, 114, 111,
    100, 34, 44, 32, 34, 116, 97, 103, 115, 34, 58, 32, 91, 91, 34, 99, 111,
    114, 114, 101, 108, 97, 116, 105, 111, 110, 73, 100, 34, 44, 32, 34, 97, 97,
    97, 97, 97, 97, 97, 97, 45, 97, 97, 97, 97, 45, 97, 97, 97, 97, 45, 97, 97,
    97, 97, 45, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 97, 34, 93, 93, 44,
    32, 34, 116, 121, 112, 101, 34, 58, 32, 34, 114, 101, 112, 108, 97, 121, 95,
    101, 118, 101, 110, 116, 34, 44, 32, 34, 117, 115, 101, 114, 34, 58, 32,
    123, 34, 101, 109, 97, 105, 108, 34, 58, 32, 34, 116, 101, 115, 116, 64,
    116, 101, 115, 116, 46, 99, 111, 109, 34, 44, 32, 34, 105, 112, 95, 97, 100,
    100, 114, 101, 115, 115, 34, 58, 32, 34, 49, 46, 49, 46, 49, 46, 49, 34,
    125, 44, 32, 34, 114, 101, 113, 117, 101, 115, 116, 34, 58, 32, 123, 34,
    117, 114, 108, 34, 58, 32, 34, 104, 116, 116, 112, 115, 58, 47, 47, 119,
    119, 119, 46, 116, 101, 115, 116, 46, 99, 111, 109, 47, 97, 115, 100, 102,
    34, 44, 32, 34, 104, 101, 97, 100, 101, 114, 115, 34, 58, 32, 91, 91, 34,
    85, 115, 101, 114, 45, 65, 103, 101, 110, 116, 34, 44, 32, 34, 77, 111, 122,
    105, 108, 108, 97, 47, 53, 46, 48, 32, 40, 87, 105, 110, 100, 111, 119, 115,
    32, 78, 84, 32, 49, 48, 46, 48, 59, 32, 87, 105, 110, 54, 52, 59, 32, 120,
    54, 52, 59, 32, 114, 118, 58, 49, 48, 57, 46, 48, 41, 32, 71, 101, 99, 107,
    111, 47, 50, 48, 49, 48, 48, 49, 48, 49, 32, 70, 105, 114, 101, 102, 111,
    120, 47, 49, 49, 50, 46, 48, 34, 93, 93, 125, 44, 32, 34, 115, 100, 107, 34,
    58, 32, 123, 34, 110, 97, 109, 101, 34, 58, 32, 34, 115, 101, 110, 116, 114,
    121, 46, 106, 97, 118, 97, 115, 99, 114, 105, 112, 116, 46, 98, 114, 111,
    119, 115, 101, 114, 34, 44, 32, 34, 118, 101, 114, 115, 105, 111, 110, 34,
    58, 32, 34, 55, 46, 53, 50, 46, 49, 34, 44, 32, 34, 105, 110, 116, 101, 103,
    114, 97, 116, 105, 111, 110, 115, 34, 58, 32, 91, 34, 84, 114, 121, 67, 97,
    116, 99, 104, 34, 44, 32, 34, 66, 114, 101, 97, 100, 99, 114, 117, 109, 98,
    115, 34, 44, 32, 34, 71, 108, 111, 98, 97, 108, 72, 97, 110, 100, 108, 101,
    114, 115, 34, 44, 32, 34, 76, 105, 110, 107, 101, 100, 69, 114, 114, 111,
    114, 115, 34, 44, 32, 34, 68, 101, 100, 117, 112, 101, 34, 44, 32, 34, 72,
    116, 116, 112, 67, 111, 110, 116, 101, 120, 116, 34, 44, 32, 34, 66, 114,
    111, 119, 115, 101, 114, 84, 114, 97, 99, 105, 110, 103, 34, 44, 32, 34, 82,
    101, 112, 108, 97, 121, 34, 93, 125, 125
  ]
}
"#,
];

const METRICS: &[&str] = &[
    r#"
{
  "version": 2,
  "mapping_meta": {
    "c": {
      "1": "g:transactions/alerts@none",
      "3": "environment",
      "5": "session.status"
    }
  },
  "metric_id": 1,
  "org_id": 1,
  "project_id": 3,
  "retention_days": 90,
  "tags": {
    "3": "production",
    "5": "init"
  },
  "timestamp": 1677512412,
  "sentry_received_timestamp": 1677519000.456,
  "type": "g",
  "use_case_id": "transactions",
  "value": {
    "min": 1.0,
    "max": 2.0,
    "sum": 3.0,
    "count": 2,
    "last": 1.0
  }
}
"#,
    r#"{
  "version": 2,
  "mapping_meta": {
    "c": {
      "1": "c:sessions/session@none",
      "3": "environment",
      "5": "session.status"
    }
  },
  "metric_id": 1,
  "org_id": 1,
  "project_id": 3,
  "retention_days": 90,
  "tags": {
    "3": "production",
    "5": "init"
  },
  "timestamp": 1677512412,
  "sentry_received_timestamp": 1677519000.456,
  "type": "c",
  "use_case_id": "performance",
  "value": 1
}
"#,
];

const OUTCOMES: &[&str] = &[
    r#"
{
  "timestamp": "2023-03-28T18:50:44.000000Z",
  "org_id": 1,
  "project_id": 1,
  "key_id": 1,
  "outcome": 1,
  "reason": "discarded-hash",
  "event_id": "4ff942d62f3f4d5db9f53b5a015b5fd9",
  "category": 1,
  "quantity": 1
}
"#,
    r#"
{
  "project_id": 1,
  "logging.googleapis.com/labels": {
    "host": "lb-6"
  },
  "org_id": 0,
  "outcome": 4,
  "timestamp": "2023-03-28T18:50:39.463685Z"
}
"#,
    r#"
{
  "timestamp": "2023-03-24T19:28:20.605851Z",
  "org_id": 1,
  "project_id": 1,
  "key_id": null,
  "outcome": 0,
  "reason": null,
  "event_id": "1410e6d2ea534dcb9d6d15a51c9962f8",
  "category": 1,
  "quantity": 1
}
"#,
    r#"
{
  "timestamp": "2023-03-28T22:51:00.000000Z",
  "project_id": 1,
  "outcome": 3,
  "reason": "project_id",
  "source": "pop-us",
  "category": 1,
  "quantity": 1
}
"#,
    r#"
{
  "timestamp": "2023-03-28T22:53:00.000000Z",
  "project_id": 1,
  "outcome": 3,
  "reason": "project_id",
  "source": "relay-internal",
  "category": 1,
  "quantity": 1
}
"#,
    r#"
{
  "org_id": 1,
  "outcome": 4,
  "project_id": 1,
  "quantity": 3,
  "timestamp": "2023-03-28T18:50:49.442341621Z"
}
"#,
];
