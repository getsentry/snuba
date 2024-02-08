use std::collections::BTreeMap;
use std::convert::TryFrom;

use crate::config::ProcessorConfig;
use anyhow::Context;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use schemars::JsonSchema;
use serde::{ser::Error, Deserialize, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use crate::types::{InsertBatch, KafkaMessageMetadata};

pub fn process_message(
    payload: KafkaPayload,
    metadata: KafkaMessageMetadata,
    _config: &ProcessorConfig,
) -> anyhow::Result<InsertBatch> {
    let payload_bytes = payload.payload().context("Expected payload")?;
    let from: FromQuerylogMessage = serde_json::from_slice(payload_bytes)?;

    let querylog_msg = QuerylogMessage {
        request: from.request,
        dataset: from.dataset,
        projects: from.projects,
        organization: from.organization,
        status: from.status,
        timing: from.timing,
        query_list: from.query_list.try_into()?,
        partition: metadata.partition,
        offset: metadata.offset,
    };

    InsertBatch::from_rows([querylog_msg], None)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct RequestBody {
    #[serde(flatten)]
    fields: BTreeMap<String, Value>,
}

fn serialize_json_str<S>(input: &RequestBody, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let request_body = serde_json::to_string(input).map_err(S::Error::custom)?;
    s.serialize_str(&request_body)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct Request {
    #[serde(rename(serialize = "request_id"))]
    id: Uuid,
    #[serde(
        rename(serialize = "request_body"),
        serialize_with = "serialize_json_str"
    )]
    body: RequestBody,
    referrer: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct Timing {
    timestamp: u64,
    duration_ms: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
struct Stats {
    #[serde(default)]
    consistent: Option<bool>,
    #[serde(default)]
    r#final: bool,
    #[serde(default)]
    cache_hit: Option<u8>,
    #[serde(default)]
    sample: Option<Value>,
    #[serde(default)]
    max_threads: Option<u8>,
    #[serde(default)]
    clickhouse_table: String,
    #[serde(default)]
    query_id: String,
    #[serde(default)]
    is_duplicate: Option<u8>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct Profile {
    time_range: Option<u32>,
    all_columns: Vec<String>,
    multi_level_condition: bool,
    where_profile: WhereProfile,
    groupby_cols: Vec<String>,
    array_join_cols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema, Default)]
#[serde(default)]
struct ResultProfile {
    bytes: u64,
    elapsed: f64,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct WhereProfile {
    columns: Vec<String>,
    mapping_cols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FromQuery {
    sql: String,
    status: String,
    trace_id: Uuid,
    stats: Stats,
    profile: Profile,
    result_profile: Option<ResultProfile>,
}

#[derive(Debug, Serialize)]
struct QueryList {
    #[serde(rename(serialize = "clickhouse_queries.sql"))]
    sql: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.status"))]
    status: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.trace_id"))]
    trace_id: Vec<Uuid>,
    #[serde(rename(serialize = "clickhouse_queries.stats"))]
    stats: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.final"))]
    r#final: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.cache_hit"))]
    cache_hit: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.sample"))]
    sample: Vec<f32>,
    #[serde(rename(serialize = "clickhouse_queries.max_threads"))]
    max_threads: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.num_days"))]
    num_days: Vec<u32>,
    #[serde(rename(serialize = "clickhouse_queries.clickhouse_table"))]
    clickhouse_table: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.query_id"))]
    query_id: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.is_duplicate"))]
    is_duplicate: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.consistent"))]
    consistent: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.all_columns"))]
    all_columns: Vec<Vec<String>>,
    #[serde(rename(serialize = "clickhouse_queries.or_conditions"))]
    or_conditions: Vec<u8>,
    #[serde(rename(serialize = "clickhouse_queries.where_columns"))]
    where_columns: Vec<Vec<String>>,
    #[serde(rename(serialize = "clickhouse_queries.where_mapping_columns"))]
    where_mapping_columns: Vec<Vec<String>>,
    #[serde(rename(serialize = "clickhouse_queries.groupby_columns"))]
    groupby_columns: Vec<Vec<String>>,
    #[serde(rename(serialize = "clickhouse_queries.array_join_columns"))]
    array_join_columns: Vec<Vec<String>>,
    #[serde(rename(serialize = "clickhouse_queries.bytes_scanned"))]
    bytes_scanned: Vec<u64>,
    #[serde(rename(serialize = "clickhouse_queries.duration_ms"))]
    duration_ms: Vec<u64>,
}

impl TryFrom<Vec<FromQuery>> for QueryList {
    type Error = anyhow::Error;
    fn try_from(from: Vec<FromQuery>) -> anyhow::Result<QueryList> {
        let mut sql = vec![];
        let mut status = vec![];
        let mut trace_id = vec![];
        let mut stats = vec![];
        let mut r#final = vec![];
        let mut cache_hit = vec![];
        let mut sample = vec![];
        let mut max_threads = vec![];
        let mut num_days = vec![];
        let mut clickhouse_table = vec![];
        let mut query_id = vec![];
        let mut is_duplicate = vec![];
        let mut consistent = vec![];
        let mut all_columns = vec![];
        let mut or_conditions = vec![];
        let mut where_columns = vec![];
        let mut where_mapping_columns = vec![];
        let mut groupby_columns = vec![];
        let mut array_join_columns = vec![];
        let mut bytes_scanned = vec![];
        let mut duration_ms = vec![];

        for q in from {
            sql.push(q.sql);
            status.push(q.status);
            trace_id.push(q.trace_id);
            r#final.push(q.stats.r#final as u8);
            cache_hit.push(q.stats.cache_hit.unwrap_or(0));
            let sample_value = q
                .stats
                .sample
                .as_ref()
                .and_then(Value::as_f64)
                .unwrap_or_default() as f32;
            sample.push(sample_value);
            max_threads.push(q.stats.max_threads.unwrap_or(0));
            num_days.push(q.profile.time_range.unwrap_or(0));
            clickhouse_table.push(q.stats.clickhouse_table.clone());
            query_id.push(q.stats.query_id.clone());
            is_duplicate.push(q.stats.is_duplicate.unwrap_or(0));
            consistent.push(q.stats.consistent.unwrap_or(false) as u8);
            all_columns.push(q.profile.all_columns);
            or_conditions.push(q.profile.multi_level_condition as u8);
            where_columns.push(q.profile.where_profile.columns);
            where_mapping_columns.push(q.profile.where_profile.mapping_cols);
            groupby_columns.push(q.profile.groupby_cols);
            array_join_columns.push(q.profile.array_join_cols);
            let result_profile = q.result_profile.unwrap_or_default();
            bytes_scanned.push(result_profile.bytes);
            duration_ms.push((result_profile.elapsed * 1000.0) as u64);

            // consistent, cache hit, max_threads and is_duplicated may not be present
            let mut sorted_stats = q.stats.extra;
            if let Some(consistent) = q.stats.consistent {
                sorted_stats.insert("consistent".to_string(), consistent.into());
            }
            sorted_stats.insert("final".to_string(), q.stats.r#final.into());
            if let Some(cache_hit) = q.stats.cache_hit {
                sorted_stats.insert("cache_hit".to_string(), cache_hit.into());
            }
            sorted_stats.insert("sample".to_string(), q.stats.sample.into());
            if let Some(max_threads) = q.stats.max_threads {
                sorted_stats.insert("max_threads".to_string(), max_threads.into());
            }
            sorted_stats.insert(
                "clickhouse_table".to_string(),
                q.stats.clickhouse_table.into(),
            );
            sorted_stats.insert("query_id".to_string(), q.stats.query_id.into());
            if let Some(is_duplicate) = q.stats.is_duplicate {
                sorted_stats.insert("is_duplicate".to_string(), is_duplicate.into());
            }

            stats.push(serde_json::to_string(&sorted_stats)?);
        }

        Ok(Self {
            sql,
            status,
            trace_id,
            stats,
            r#final,
            cache_hit,
            sample,
            max_threads,
            num_days,
            clickhouse_table,
            query_id,
            is_duplicate,
            consistent,
            all_columns,
            or_conditions,
            where_columns,
            where_mapping_columns,
            groupby_columns,
            array_join_columns,
            bytes_scanned,
            duration_ms,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FromQuerylogMessage {
    request: Request,
    dataset: String,
    projects: Vec<u64>,
    organization: Option<u64>,
    status: String,
    timing: Timing,
    query_list: Vec<FromQuery>,
}

#[derive(Debug, Serialize)]
struct QuerylogMessage {
    #[serde(flatten)]
    request: Request,
    dataset: String,
    projects: Vec<u64>,
    organization: Option<u64>,
    status: String,
    partition: u16,
    offset: u64,
    #[serde(flatten)]
    timing: Timing,
    #[serde(flatten)]
    query_list: QueryList,
}

#[cfg(test)]
mod tests {
    use crate::processors::tests::run_schema_type_test;

    use super::*;

    use chrono::DateTime;
    use rust_arroyo::backends::kafka::types::KafkaPayload;
    use std::time::SystemTime;

    #[test]
    fn test_querylog() {
        let data = r#"{
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
          }"#;

        let payload = KafkaPayload::new(None, None, Some(data.as_bytes().to_vec()));
        let meta = KafkaMessageMetadata {
            partition: 0,
            offset: 1,
            timestamp: DateTime::from(SystemTime::now()),
        };
        process_message(payload, meta, &ProcessorConfig::default())
            .expect("The message should be processed");
    }

    #[test]
    fn schema() {
        run_schema_type_test::<FromQuerylogMessage>("snuba-queries");
    }
}
