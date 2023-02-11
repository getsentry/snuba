use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};

pub type RawQueryLogKafkaJson = HashMap<String, Value>;
type QueryList = HashMap<String, Vec<Value>>;
// TODO prob move to a separate file
#[derive(Default,Debug, Deserialize, Serialize)]
pub struct ProcessedQueryLog
{
    request_id: String,
    request_body: String,
    referrer: String,
    dataset: String,
    projects: Vec<String>,
    organization: Option<String>,
    query_list: QueryList,

}

fn _processed_querylog_to_json(processed_querylog: &ProcessedQueryLog) -> String {
    let mut json_map = HashMap::new();
    json_map.insert("request_id".to_string(), Value::String(processed_querylog.request_id.clone()));
    json_map.insert("request_body".to_string(), Value::String(processed_querylog.request_body.clone()));
    json_map.insert("referrer".to_string(), Value::String(processed_querylog.referrer.clone()));
    json_map.insert("dataset".to_string(), Value::String(processed_querylog.dataset.clone()));
    json_map.insert("projects".to_string(), Value::Array(processed_querylog.projects.iter().map(|x| Value::String(x.clone())).collect()));
    json_map.insert("organization".to_string(), Value::String(processed_querylog.organization.clone().unwrap_or("".to_string())));

    for (k,v) in &processed_querylog.query_list {
        json_map.insert(k.to_string(), Value::Array(v.clone()));
    }
    let json_arr = vec![serde_json::to_value(json_map).unwrap()];

    return serde_json::to_string(&json_arr).unwrap();
}


fn _remove_invalid_pid(project_id: &String) -> bool {
    let pid =  project_id.parse::<i32>();
    match pid {
        Ok(pid) => {
            if pid <= 0 {
                return false;
            }
            return true;
        }
        Err(_) => {
            return false;
        }
    }
}

fn _push_or_create_query_list(query_list_map: &mut QueryList, key: &str, query_list_item: Value) {
    let query_list = query_list_map.get_mut(key);
    match query_list {
        Some(query_list) => {
            query_list.push(query_list_item);
        }
        None => {
            query_list_map.insert(key.to_string(), vec![query_list_item]);
        }
    }
}

fn extract_query_list(raw_query_list_map: &Vec<Map<String,Value>>, processed_query_log: &mut ProcessedQueryLog) {
    for query_list_item in raw_query_list_map  {
        // TODO prob can get rid of this clone with lifetimes
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse_queries.sql", query_list_item["sql"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse_queries.status", query_list_item["status"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse_queries.trace_id", query_list_item["trace_id"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.duration_ms", query_list_item["duration_ms"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.stats", query_list_item["stats"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.final", query_list_item["final"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.cache_hit", query_list_item["cache_hit"].clone());
        // // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.sample", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.max_threads", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.num_days", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.clickhouse_table", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.query_id", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.is_duplicate", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.consistent", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.all_columns", query_list_item["cache_hit"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.clickhouse_table", query_list_item["cache_hit"].clone());

        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.bytes_scanned", query_list_item["bytes_scanned"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.or_conditions", query_list_item["or_conditions"].clone());
        // _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.stats", query_list_item["stats"].clone());

    }
}

pub fn remove_invalid_data(processed_query_log: &mut ProcessedQueryLog) {
    // TODO
    // processed_query_log
    processed_query_log.projects.retain(_remove_invalid_pid);
}

// unwraps string safely from json using the default value if the key is not present
macro_rules! unwrap_string {
    ($obj:expr, $key:expr) => {
        $obj.get($key).unwrap().as_str().unwrap_or_default().to_string()
    };
}

macro_rules! unwrap_request {
    ($obj:expr, $key:expr) => {
        $obj.get("request").unwrap()
        .as_object().unwrap()
        .get($key).unwrap()
    };
}

pub fn process(message: RawQueryLogKafkaJson) -> String {
    let mut result = ProcessedQueryLog::default();
    result.request_id = message.get("request").unwrap()
                            .as_object().unwrap()
                            .get("id").unwrap()
                            .as_str().unwrap_or_default().to_string();

    // let obj: HashMap<String, Value> = HashMap::new();
    let unwrapped_body = unwrap_request!( message, "body").as_object();
    result.request_body =serde_json::to_string(&unwrapped_body).unwrap_or_default();
    result.referrer = unwrap_request!( message, "referrer").as_str().unwrap_or_default().to_string();
    result.dataset = unwrap_string!( message, "dataset");

    if message.get("organization").is_some() {
        result.organization = Some(message.get("organization").unwrap().as_str().unwrap_or_default().to_string());
    }
    match message.get("query_list") {
        Some(query_list) => {

            let query_list_it = query_list.as_array().unwrap().iter();
            let raw_query_list_map: Vec<Map<String, Value>> = query_list_it.map(|x| x.as_object().unwrap().clone()).collect();
            extract_query_list(&raw_query_list_map, &mut result);
            return _processed_querylog_to_json(&result);
        }
        None => todo!(),
    }
    // let raw_query_list_map: Vec<HashMap<String, Value>> = message.get("query_list").unwrap().as_array().unwrap();


}
