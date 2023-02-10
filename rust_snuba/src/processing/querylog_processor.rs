use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};

pub type RawQueryLogKafkaJson = HashMap<String, Value>;
type QueryList = HashMap<String, Vec<Value>>;
// int x = 5;
// TODO prob move to a separate file
#[derive(Debug, Deserialize, Serialize)]
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
        // processed_query_log.query_list.get_mut("query_list").unwrap().push(query_list_item.clone());
        _push_or_create_query_list(&mut processed_query_log.query_list, "clickhouse.sql", query_list_item["sql"].clone())
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

pub fn process(message: RawQueryLogKafkaJson, result: &mut ProcessedQueryLog ) {

    result.request_id = message.get("request_id").unwrap().as_str().unwrap_or_default().to_string();
    let obj: HashMap<String, Value> = HashMap::new();
    result.request_body = unwrap_string!( obj, "request_body");
    result.referrer = unwrap_string!( obj, "referrer");
    result.dataset = unwrap_string!( obj, "dataset");



    result.organization = Some(message.get("organization").unwrap().as_str().unwrap_or_default().to_string());
    match message.get("query_list") {
        Some(query_list) => {
            //let raw_query_list_map: Vec<HashMap<String, Value>> = query_list.as_array().unwrap().iter().map(|x| x.as_object().unwrap()).collect();
            let query_list_it = query_list.as_array().unwrap().iter();
            let raw_query_list_map: Vec<Map<String, Value>> = query_list_it.map(|x| x.as_object().unwrap().clone()).collect();
            //.map(|x| x.as_object().unwrap());
            extract_query_list(&raw_query_list_map, result)
            // result.query_list = HashMap::new();
        }
        None => todo!(),
    }
    // let raw_query_list_map: Vec<HashMap<String, Value>> = message.get("query_list").unwrap().as_array().unwrap();


}
