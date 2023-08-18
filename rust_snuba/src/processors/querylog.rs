use crate::types::BytesInsertBatch;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::processing::strategies::InvalidMessage;
use serde::{ser::Error, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use uuid::Uuid;

pub fn process_message(payload: KafkaPayload) -> Result<BytesInsertBatch, InvalidMessage> {
    if let Some(payload_bytes) = payload.payload {
        let msg: FromQuerylogMessage =
            serde_json::from_slice(&payload_bytes).map_err(|_| InvalidMessage)?;
        let querylog_msg: QuerylogMessage = msg.try_into()?;

        let serialized = serde_json::to_vec(&querylog_msg).map_err(|_| InvalidMessage)?;

        return Ok(BytesInsertBatch {
            rows: vec![serialized],
        });
    }

    Err(InvalidMessage)
}

#[derive(Debug, Deserialize, Serialize)]
struct RequestBody {
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}

fn serialize_uuid<S>(input: &str, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let uuid = Uuid::parse_str(input)
        .map_err(S::Error::custom)?
        .to_string();
    s.serialize_str(&uuid)
}

fn nullable_result_profile<'de, D>(deserializer: D) -> Result<ResultProfile, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_else(ResultProfile::default))
}

#[derive(Debug, Deserialize, Serialize)]
struct Request {
    #[serde(rename(serialize = "request_id"), serialize_with = "serialize_uuid")]
    id: String,
    #[serde(rename(serialize = "request_body"))]
    body: RequestBody,
    referrer: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Timing {
    timestamp: u64,
    duration_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Stats {
    #[serde(default)]
    consistent: bool,
    #[serde(default)]
    r#final: bool,
    #[serde(default)]
    cache_hit: u8,
    #[serde(default)]
    sample: Option<f32>,
    #[serde(default)]
    max_threads: u8,
    #[serde(default)]
    clickhouse_table: String,
    #[serde(default)]
    query_id: String,
    #[serde(default)]
    is_duplicate: u8,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct Profile {
    time_range: Option<u32>,
    all_columns: Vec<String>,
    multi_level_condition: bool,
    where_profile: WhereProfile,
    groupby_cols: Vec<String>,
    array_join_cols: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ResultProfile {
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    elapsed: f64,
}

impl Default for ResultProfile {
    fn default() -> Self {
        ResultProfile {
            bytes: 0,
            elapsed: 0.0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WhereProfile {
    columns: Vec<String>,
    mapping_cols: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FromQuery {
    sql: String,
    status: String,
    trace_id: String,
    stats: Stats,
    profile: Profile,
    #[serde(default, deserialize_with = "nullable_result_profile")]
    result_profile: ResultProfile,
}

#[derive(Debug, Serialize)]
struct QueryList {
    #[serde(rename(serialize = "clickhouse_queries.sql"))]
    sql: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.status"))]
    status: Vec<String>,
    #[serde(rename(serialize = "clickhouse_queries.trace_id"))]
    trace_id: Vec<String>,
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
    type Error = InvalidMessage;
    fn try_from(from: Vec<FromQuery>) -> Result<QueryList, InvalidMessage> {
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
            trace_id.push(
                Uuid::parse_str(&q.trace_id)
                    .map_err(|_| InvalidMessage)?
                    .to_string(),
            );
            stats.push(serde_json::to_string(&q.stats).map_err(|_| InvalidMessage)?);
            r#final.push(q.stats.r#final as u8);
            cache_hit.push(q.stats.cache_hit);
            sample.push(q.stats.sample.unwrap_or(0.0));
            max_threads.push(q.stats.max_threads);
            num_days.push(q.profile.time_range.unwrap_or(0));
            clickhouse_table.push(q.stats.clickhouse_table);
            query_id.push(q.stats.query_id);
            is_duplicate.push(q.stats.is_duplicate);
            consistent.push(q.stats.consistent as u8);
            all_columns.push(q.profile.all_columns);
            or_conditions.push(q.profile.multi_level_condition as u8);
            where_columns.push(q.profile.where_profile.columns);
            where_mapping_columns.push(q.profile.where_profile.mapping_cols);
            groupby_columns.push(q.profile.groupby_cols);
            array_join_columns.push(q.profile.array_join_cols);
            bytes_scanned.push(q.result_profile.bytes);
            duration_ms.push((q.result_profile.elapsed * 1000.0) as u64);
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

#[derive(Debug, Deserialize)]
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
    #[serde(flatten)]
    timing: Timing,
    #[serde(flatten)]
    query_list: QueryList,
}

impl TryFrom<FromQuerylogMessage> for QuerylogMessage {
    type Error = InvalidMessage;
    fn try_from(from: FromQuerylogMessage) -> Result<QuerylogMessage, InvalidMessage> {
        Ok(Self {
            request: from.request,
            dataset: from.dataset,
            projects: from.projects,
            organization: from.organization,
            status: from.status,
            timing: from.timing,
            query_list: from.query_list.try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_arroyo::backends::kafka::types::KafkaPayload;

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

        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(data.as_bytes().to_vec()),
        };
        process_message(payload).expect("The message should be processed");
    }
}
